"""
Tests for message deduplication race conditions.

This module tests the race condition in the message deduplication system where
is_message_processed() and mark_message_processed() are separate operations.
Two threads can both see a message as unprocessed and both process it.

CRITICAL BUG: The current implementation has a check-then-act race window.
"""

import threading
import time
from typing import Any

import pytest

from stabilize.errors import DuplicateDeliveryError
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
            from stabilize.persistence.connection import (
                ConnectionManager,
                SingletonMeta,
            )
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
            except DuplicateDeliveryError:
                with lock:
                    results.append(False)

        # Run two threads trying to mark the same message
        t1 = threading.Thread(target=mark_message)
        t2 = threading.Thread(target=mark_message)

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert sorted(results) == [False, True], f"exactly one mark must win: {results}"

        if backend == "sqlite":
            test_store.close()
            test_queue.close()

    def test_claim_message_admits_exactly_one_worker(
        self, repository: WorkflowStore, queue: Queue, backend: str, tmp_path: Any
    ) -> None:
        """Two workers claim the same message at once; the store admits exactly one of them."""
        if backend == "sqlite":
            from stabilize.persistence.connection import ConnectionManager, SingletonMeta
            from stabilize.persistence.sqlite import SqliteWorkflowStore

            SingletonMeta.reset(ConnectionManager)
            store = SqliteWorkflowStore(f"sqlite:///{tmp_path / 'claim.db'}", create_tables=True)
        else:
            store = repository
        message_id = "test-msg-claim-race"
        barrier = threading.Barrier(2)
        results: list[bool] = []
        lock = threading.Lock()

        def claim() -> None:
            barrier.wait()
            won = store.claim_message(message_id, "TestHandler", "exec-1")
            with lock:
                results.append(won)

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert sorted(results) == [False, True], f"claim_message must admit exactly one worker: {results}"
        assert store.is_message_processed(message_id)
        if backend == "sqlite":
            store.close()

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
            is_processed = is_message_processed(conn, message_id, "")

            with check_lock:
                check_results.append(is_processed)

            if not is_processed:
                # Small delay to widen the race window
                time.sleep(0.01)
                mark_message_processed(conn, message_id, "", f"Thread-{thread_id}", "exec-1")

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
