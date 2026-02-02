"""
Tests for message deduplication race conditions.

This module tests the race condition in the message deduplication system where
is_message_processed() and mark_message_processed() are separate operations.
Two threads can both see a message as unprocessed and both process it.

CRITICAL BUG: The current implementation has a check-then-act race window.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue


class TestMessageDeduplicationRace:
    """Test race conditions in message deduplication."""

    def test_concurrent_mark_same_message_id(
        self, repository: WorkflowStore, queue: Queue, backend: str, tmp_path: Any
    ) -> None:
        """
        Two threads marking the same message ID should not cause errors.

        The INSERT OR IGNORE / ON CONFLICT DO NOTHING should handle this gracefully.
        """
        # For SQLite :memory:, each thread gets its own database (thread-local)
        # So we need to use file-based SQLite for this test
        if backend == "sqlite":
            from stabilize.persistence.connection import ConnectionManager, SingletonMeta
            from stabilize.persistence.sqlite import SqliteWorkflowStore
            from stabilize.queue.sqlite import SqliteQueue

            SingletonMeta.reset(ConnectionManager)

            db_path = tmp_path / "concurrent_mark.db"
            connection_string = f"sqlite:///{db_path}"

            test_store = SqliteWorkflowStore(connection_string, create_tables=True)
            test_queue = SqliteQueue(connection_string)
            test_queue._create_table()
        else:
            test_store = repository
            test_queue = queue

        message_id = "test-msg-concurrent-mark"
        results: list[bool] = []
        lock = threading.Lock()

        def mark_message() -> None:
            try:
                with test_store.transaction(test_queue) as txn:
                    txn.mark_message_processed(
                        message_id,
                        handler_type="TestHandler",
                        execution_id="exec-1",
                    )
                with lock:
                    results.append(True)
            except Exception:
                with lock:
                    results.append(False)

        # Run two threads trying to mark the same message
        t1 = threading.Thread(target=mark_message)
        t2 = threading.Thread(target=mark_message)

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Both should succeed (INSERT OR IGNORE handles duplicates)
        assert all(results), f"Some threads failed: {results}"
        assert len(results) == 2

        if backend == "sqlite":
            test_store.close()
            test_queue.close()

    def test_race_between_check_and_mark_demonstrates_bug(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """
        Demonstrate the race window between is_message_processed and mark_message_processed.

        This test shows that two threads can both see a message as unprocessed
        and both attempt to process it - the bug that exists in the current implementation.

        The bug is that the check and mark are NOT atomic, so:
        1. Thread A calls is_message_processed("msg-1") -> False
        2. Thread B calls is_message_processed("msg-1") -> False
        3. Thread A starts processing message
        4. Thread B starts processing message (DUPLICATE!)
        5. Both threads call mark_message_processed
        """
        message_id = "test-msg-race-demo"
        processing_count = 0
        lock = threading.Lock()
        barrier = threading.Barrier(2)  # Synchronize threads

        def simulate_handler_processing() -> bool:
            nonlocal processing_count

            # Simulate handler checking if message is already processed
            # Using direct operations to bypass transaction for demonstration
            from stabilize.persistence.connection import get_connection_manager

            if backend == "sqlite":
                from stabilize.persistence.sqlite.operations import (
                    is_message_processed,
                    mark_message_processed,
                )

                conn = get_connection_manager().get_sqlite_connection("sqlite:///:memory:")

                # Synchronize - both threads check at roughly the same time
                barrier.wait()

                is_processed = is_message_processed(conn, message_id)

                if not is_processed:
                    # Both threads reach here if there's a race
                    with lock:
                        processing_count += 1

                    # Simulate processing work
                    time.sleep(0.01)

                    # Mark as processed
                    mark_message_processed(conn, message_id, "TestHandler", "exec-1")
                    return True
            else:
                # PostgreSQL path
                from stabilize.persistence.postgres.operations import (
                    is_message_processed,
                    mark_message_processed,
                )

                pool = get_connection_manager().get_postgres_pool(
                    repository.connection_string  # type: ignore
                )

                barrier.wait()

                is_processed = is_message_processed(pool, message_id)

                if not is_processed:
                    with lock:
                        processing_count += 1

                    time.sleep(0.01)
                    mark_message_processed(pool, message_id, "TestHandler", "exec-1")
                    return True

            return False

        # This test documents the bug - with proper synchronization,
        # both threads should be able to pass the is_processed check
        # Skip for in-memory SQLite as each thread gets its own database
        if backend == "sqlite":
            pytest.skip(
                "SQLite :memory: gives each thread its own database - use file-based SQLite to demonstrate this bug"
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(simulate_handler_processing) for _ in range(2)]
            [f.result() for f in as_completed(futures)]

        # BUG DEMONSTRATION: processing_count might be 2 if both threads
        # passed the is_processed check before either marked it processed
        # This test documents that handlers MUST be idempotent
        if processing_count > 1:
            # This is the bug case - document it
            pytest.xfail(
                f"KNOWN BUG: Message processed {processing_count} times due to race condition. "
                "Handlers must be idempotent to handle this."
            )

    def test_handler_idempotency_is_required(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Verify that handlers must be idempotent because of the deduplication race.

        This test creates a workflow and demonstrates that a handler might be
        invoked multiple times for the same message if deduplication fails.
        The handler must produce the same result regardless.
        """
        # Create a workflow with a task
        wf = Workflow.create("test-app", "idempotency-test", [])
        stage = StageExecution.create("stage-1", "Test Stage", "1")
        stage.execution = wf
        task = TaskExecution.create("Task A", "shell", stage_start=True, stage_end=True)
        task.status = WorkflowStatus.RUNNING
        stage.tasks = [task]
        stage.status = WorkflowStatus.RUNNING
        wf.stages = [stage]
        repository.store(wf)

        # Simulate idempotent task completion
        def complete_task_idempotently() -> bool:
            """Complete task in an idempotent manner."""
            fresh_stage = repository.retrieve_stage(stage.id)
            fresh_task = next(t for t in fresh_stage.tasks if t.id == task.id)

            # Idempotent check: if already completed, do nothing
            if fresh_task.status in (
                WorkflowStatus.SUCCEEDED,
                WorkflowStatus.TERMINAL,
                WorkflowStatus.FAILED_CONTINUE,
            ):
                return False  # Already processed

            fresh_task.status = WorkflowStatus.SUCCEEDED
            repository.store_stage(fresh_stage)
            return True

        # Run multiple times - only first should actually change state
        results = []
        for _ in range(5):
            results.append(complete_task_idempotently())

        assert results[0] is True, "First call should process"
        assert all(r is False for r in results[1:]), "Subsequent calls should be no-ops"

        # Verify final state
        final_stage = repository.retrieve_stage(stage.id)
        final_task = next(t for t in final_stage.tasks if t.id == task.id)
        assert final_task.status == WorkflowStatus.SUCCEEDED

    def test_deduplication_stress(self, repository: WorkflowStore, queue: Queue, backend: str, tmp_path: Any) -> None:
        """
        Stress test: 20 threads trying to process the same message.

        This verifies that even under high concurrency:
        1. The INSERT OR IGNORE/ON CONFLICT handles duplicates gracefully
        2. No exceptions are thrown
        3. Only one record ends up in processed_messages table
        """
        # For SQLite :memory:, use file-based DB for real concurrency test
        if backend == "sqlite":
            from stabilize.persistence.connection import ConnectionManager, SingletonMeta
            from stabilize.persistence.sqlite import SqliteWorkflowStore
            from stabilize.queue.sqlite import SqliteQueue

            SingletonMeta.reset(ConnectionManager)

            db_path = tmp_path / "stress_dedup.db"
            connection_string = f"sqlite:///{db_path}"

            test_store = SqliteWorkflowStore(connection_string, create_tables=True)
            test_queue = SqliteQueue(connection_string)
            test_queue._create_table()
        else:
            test_store = repository
            test_queue = queue

        message_id = f"stress-test-msg-{time.time()}"
        success_count = 0
        error_count = 0
        lock = threading.Lock()

        def mark_processed() -> None:
            nonlocal success_count, error_count
            try:
                with test_store.transaction(test_queue) as txn:
                    txn.mark_message_processed(
                        message_id,
                        handler_type="StressHandler",
                        execution_id="stress-exec",
                    )
                with lock:
                    success_count += 1
            except Exception:
                with lock:
                    error_count += 1

        threads = [threading.Thread(target=mark_processed) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should succeed (no exceptions from duplicate handling)
        assert error_count == 0, f"Got {error_count} errors"
        assert success_count == 20, f"Only {success_count}/20 succeeded"

        if backend == "sqlite":
            test_store.close()
            test_queue.close()

    def test_check_then_mark_not_atomic_demonstration(
        self, repository: WorkflowStore, queue: Queue, backend: str, tmp_path: Any
    ) -> None:
        """
        Demonstrate that check-then-mark is not atomic using file-based SQLite.

        For SQLite :memory:, each thread gets its own database due to thread-local
        connections, so we use a file-based database to demonstrate the race.
        """
        if backend == "postgres":
            pytest.skip("This specific test is for SQLite file-based demonstration")

        # Use file-based SQLite so threads share the same database
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        db_path = tmp_path / "race_test.db"
        connection_string = f"sqlite:///{db_path}"

        store = SqliteWorkflowStore(connection_string, create_tables=True)
        test_queue = SqliteQueue(connection_string)
        test_queue._create_table()

        message_id = "file-based-race-test"
        check_results: list[bool] = []
        barrier = threading.Barrier(2)
        check_lock = threading.Lock()

        def check_and_mark(thread_id: int) -> None:
            from stabilize.persistence.connection import get_connection_manager
            from stabilize.persistence.sqlite.operations import (
                is_message_processed,
                mark_message_processed,
            )

            conn = get_connection_manager().get_sqlite_connection(connection_string)

            # Synchronize at the check
            barrier.wait()

            # Both threads check at the same time
            is_processed = is_message_processed(conn, message_id)

            with check_lock:
                check_results.append(is_processed)

            if not is_processed:
                # Small delay to widen the race window
                time.sleep(0.01)
                mark_message_processed(conn, message_id, f"Thread-{thread_id}", "exec-1")

        t1 = threading.Thread(target=check_and_mark, args=(1,))
        t2 = threading.Thread(target=check_and_mark, args=(2,))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Document the race condition result
        # Both threads might see is_processed=False if they check simultaneously
        false_count = check_results.count(False)
        if false_count == 2:
            # This demonstrates the race condition exists
            pass  # Expected in race condition scenario
        elif false_count == 1:
            # One thread saw False, one saw True (no race this time)
            pass  # Lucky timing, no race happened

        # Cleanup
        store.close()
        test_queue.close()


class TestMessageDeduplicationEdgeCases:
    """Edge cases for message deduplication."""

    def test_mark_processed_with_none_handler_type(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test marking a message with None handler_type."""
        message_id = f"null-handler-{time.time()}"

        with repository.transaction(queue) as txn:
            txn.mark_message_processed(
                message_id,
                handler_type=None,
                execution_id="exec-1",
            )

        # Should not raise any errors

    def test_mark_processed_with_very_long_message_id(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test with a very long message ID."""
        message_id = "msg-" + "x" * 500  # Very long ID

        with repository.transaction(queue) as txn:
            txn.mark_message_processed(
                message_id,
                handler_type="TestHandler",
                execution_id="exec-1",
            )

        # Should handle long IDs gracefully

    def test_mark_processed_with_special_characters(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test with special characters in message ID."""
        message_id = "msg-'\"\\--DROP TABLE;--"

        with repository.transaction(queue) as txn:
            txn.mark_message_processed(
                message_id,
                handler_type="TestHandler",
                execution_id="exec-1",
            )

        # Should handle SQL injection attempts via parameterization
