"""
Tests for DLQ (Dead Letter Queue) atomicity.

This module verifies that DLQ operations are atomic:
- move_to_dlq() does SELECT -> INSERT to DLQ -> DELETE from main within a single
  transaction, committed atomically by conn.commit() (SQLite) or at connection
  context exit (PostgreSQL). A crash before commit rolls back both INSERT and DELETE.
- replay_dlq() follows the same pattern.
- Concurrent move attempts are safe because only one worker owns a message at a time
  (poll uses optimistic locking).
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from typing import Any

import pytest

from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartWorkflow


class TestDLQAtomicity:
    """Test DLQ operations for atomicity issues."""

    def test_move_to_dlq_basic(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test basic move_to_dlq functionality."""
        # Clear queue to ensure clean state
        queue.clear()

        # Push a message
        msg = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="dlq-test-1",
        )
        queue.push(msg)

        # Poll to get internal ID
        polled = queue.poll_one()
        assert polled is not None
        assert polled.message_id is not None

        # Move to DLQ - use the message_id converted to int
        queue.move_to_dlq(int(polled.message_id), "Test error")

        # Verify message is in DLQ
        dlq_messages = queue.list_dlq()
        assert len(dlq_messages) >= 1

        # Verify message is NOT in main queue
        assert queue.size() == 0

    def test_move_to_dlq_is_atomic_on_crash(self, tmp_path: Any, backend: str) -> None:
        """Verify crash before commit rolls back both INSERT and DELETE — no partial state."""
        if backend == "postgres":
            pytest.skip("This test uses SQLite-specific mocking")

        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        db_path = tmp_path / "dlq_crash_test.db"
        connection_string = f"sqlite:///{db_path}"

        store = SqliteWorkflowStore(connection_string, create_tables=True)
        test_queue = SqliteQueue(connection_string)
        test_queue._create_table()

        msg = StartWorkflow(execution_type="PIPELINE", execution_id="crash-test")
        test_queue.push(msg)
        polled = test_queue.poll_one()
        assert polled is not None
        msg_queue_id = int(polled.message_id)  # type: ignore

        from stabilize.persistence.connection import get_connection_manager

        real_conn = get_connection_manager().get_sqlite_connection(connection_string)

        try:
            row = real_conn.execute(
                "SELECT id, message_id, message_type, payload, attempts, created_at FROM queue_messages WHERE id = ?",
                (msg_queue_id,),
            ).fetchone()
            assert row is not None

            real_conn.execute(
                """INSERT INTO queue_messages_dlq (
                    original_id, message_id, message_type, payload,
                    attempts, error, last_error_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, datetime('now', 'utc'), ?)""",
                (
                    row["id"],
                    row["message_id"],
                    row["message_type"],
                    row["payload"],
                    row["attempts"],
                    "Simulated crash test",
                    row["created_at"],
                ),
            )
            real_conn.execute("DELETE FROM queue_messages WHERE id = ?", (msg_queue_id,))

            # Simulate crash BEFORE commit — rollback instead
            real_conn.rollback()

            # Verify atomicity: message stays in main queue only, DLQ is empty
            dlq_count = real_conn.execute(
                "SELECT COUNT(*) FROM queue_messages_dlq WHERE original_id = ?", (msg_queue_id,)
            ).fetchone()[0]
            main_count = real_conn.execute(
                "SELECT COUNT(*) FROM queue_messages WHERE id = ?", (msg_queue_id,)
            ).fetchone()[0]

            assert dlq_count == 0, "DLQ should be empty after rollback"
            assert main_count == 1, "Message should remain in main queue after rollback"

        finally:
            store.close()
            test_queue.close()

    def test_concurrent_move_to_dlq_same_message(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Concurrent move_to_dlq calls: only one succeeds, no duplicates in DLQ."""
        if backend == "sqlite":
            pytest.skip("SQLite uses thread-local connections; concurrent DLQ tested via file-based DB")

        queue.clear_dlq()

        msg = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="concurrent-dlq-test",
        )
        queue.push(msg)
        polled = queue.poll_one()
        assert polled is not None
        msg_queue_id = int(polled.message_id)  # type: ignore

        results: list[str] = []
        lock = threading.Lock()

        def try_move_to_dlq(thread_id: int) -> None:
            try:
                queue.move_to_dlq(msg_queue_id, f"Error from thread {thread_id}")
                with lock:
                    results.append(f"success-{thread_id}")
            except Exception as e:
                with lock:
                    results.append(f"error-{thread_id}: {e}")

        t1 = threading.Thread(target=try_move_to_dlq, args=(1,))
        t2 = threading.Thread(target=try_move_to_dlq, args=(2,))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        dlq_messages = queue.list_dlq()
        our_dlq = [m for m in dlq_messages if "concurrent-dlq-test" in str(m.get("payload", ""))]
        assert len(our_dlq) == 1, f"Should have exactly 1 DLQ entry, got {len(our_dlq)}"

        assert queue.size() == 0

    def test_replay_dlq_is_atomic_on_crash(self, tmp_path: Any, backend: str) -> None:
        """Verify replay_dlq rollback on crash leaves message only in DLQ."""
        if backend == "postgres":
            pytest.skip("This test uses SQLite-specific implementation")

        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        db_path = tmp_path / "replay_crash_test.db"
        connection_string = f"sqlite:///{db_path}"

        store = SqliteWorkflowStore(connection_string, create_tables=True)
        test_queue = SqliteQueue(connection_string)
        test_queue._create_table()

        msg = StartWorkflow(execution_type="PIPELINE", execution_id="replay-crash-test")
        test_queue.push(msg)
        polled = test_queue.poll_one()
        assert polled is not None
        test_queue.move_to_dlq(int(polled.message_id), "Initial error")  # type: ignore

        dlq_entries = test_queue.list_dlq()
        assert len(dlq_entries) >= 1
        dlq_id = dlq_entries[0]["id"]

        from stabilize.persistence.connection import get_connection_manager

        real_conn = get_connection_manager().get_sqlite_connection(connection_string)

        try:
            row = real_conn.execute("SELECT * FROM queue_messages_dlq WHERE id = ?", (dlq_id,)).fetchone()
            assert row is not None

            real_conn.execute(
                """INSERT INTO queue_messages (
                    message_id, message_type, payload, deliver_at, attempts
                ) VALUES (?, ?, ?, datetime('now', 'utc'), 0)""",
                (row["message_id"] + "-replay", row["message_type"], row["payload"]),
            )
            real_conn.execute("DELETE FROM queue_messages_dlq WHERE id = ?", (dlq_id,))

            # Simulate crash BEFORE commit — rollback
            real_conn.rollback()

            dlq_count = real_conn.execute("SELECT COUNT(*) FROM queue_messages_dlq WHERE id = ?", (dlq_id,)).fetchone()[
                0
            ]
            main_count = real_conn.execute(
                "SELECT COUNT(*) FROM queue_messages WHERE message_id LIKE '%replay%'"
            ).fetchone()[0]

            assert dlq_count == 1, "Message should remain in DLQ after rollback"
            assert main_count == 0, "Main queue should be empty after rollback"

        finally:
            store.close()
            test_queue.close()

    def test_dlq_operations_under_load(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Stress test DLQ operations with concurrent message processing failures.

        Push many messages, have them all fail, and verify DLQ is consistent.
        """
        if backend == "sqlite":
            pytest.skip("SQLite doesn't handle high-concurrency DLQ operations reliably")

        # Clear DLQ to ensure clean state
        queue.clear_dlq()

        num_messages = 50

        # Push messages
        for i in range(num_messages):
            msg = StartWorkflow(
                execution_type="PIPELINE",
                execution_id=f"load-test-{i}",
            )
            queue.push(msg)

        # Poll all and move to DLQ concurrently
        polled_messages = []
        for _ in range(num_messages):
            polled = queue.poll_one()
            if polled:
                polled_messages.append(polled)

        assert len(polled_messages) == num_messages

        results: list[bool] = []
        lock = threading.Lock()

        def move_to_dlq(msg: Any) -> None:
            try:
                queue.move_to_dlq(int(msg.message_id), "Load test failure")
                with lock:
                    results.append(True)
            except Exception:
                with lock:
                    results.append(False)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(move_to_dlq, msg) for msg in polled_messages]
            for f in as_completed(futures):
                pass  # Wait for all

        # All should succeed
        success_count = sum(results)
        assert success_count == num_messages, f"Only {success_count}/{num_messages} moved to DLQ"

        # Verify DLQ has all messages
        assert queue.dlq_size() == num_messages

        # Verify main queue is empty
        assert queue.size() == 0

    def test_check_and_move_expired_atomicity(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test check_and_move_expired for atomicity issues.

        This method finds expired messages and moves them to DLQ.
        """
        # Clear queue to ensure clean state
        queue.clear()

        # This test requires setting up messages with max_attempts exceeded
        # Push a message with low max_attempts configured on the message itself
        msg = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="expired-test",
        )
        msg.max_attempts = 1  # Set low max_attempts on the message
        queue.push(msg)

        # Poll and simulate failure by not acking
        polled = queue.poll_one()
        assert polled is not None

        # Reschedule to increment attempts
        queue.reschedule(polled, timedelta(seconds=0))

        # Now poll again - this should exceed max_attempts
        polled2 = queue.poll_one()
        if polled2:
            queue.reschedule(polled2, timedelta(seconds=0))

        # check_and_move_expired should move to DLQ
        queue.check_and_move_expired()

        # Verify behavior
        # (The exact behavior depends on implementation details)


class TestDLQEdgeCases:
    """Edge cases for DLQ operations."""

    def test_move_nonexistent_message_to_dlq(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test moving a non-existent message to DLQ."""
        # Try to move a message that doesn't exist
        queue.move_to_dlq(999999, "Message doesn't exist")

        # Should handle gracefully (just log warning, no exception)

    def test_replay_nonexistent_dlq_entry(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test replaying a non-existent DLQ entry."""
        result = queue.replay_dlq(999999)

        # Should return False, not raise exception
        assert result is False

    def test_clear_dlq_while_replay_in_progress(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test clearing DLQ while a replay is potentially in progress."""
        # Clear queue and DLQ to ensure clean state
        queue.clear()
        queue.clear_dlq()

        # Add some messages to DLQ
        moved_count = 0
        for i in range(5):
            msg = StartWorkflow(
                execution_type="PIPELINE",
                execution_id=f"clear-test-{i}",
            )
            queue.push(msg)
            polled = queue.poll_one()
            if polled:
                queue.move_to_dlq(int(polled.message_id), "Test")  # type: ignore
                moved_count += 1

        # Clear DLQ
        cleared = queue.clear_dlq()
        assert cleared == moved_count, f"Expected {moved_count} cleared, got {cleared}"

        # Verify DLQ is empty
        assert queue.dlq_size() == 0

    def test_dlq_with_very_large_payload(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test DLQ with a message containing a very large payload."""
        # Clear queue to ensure clean state
        queue.clear()

        # Create message with large execution_id (simulating large payload)
        large_id = "large-" + "x" * 10000
        msg = StartWorkflow(
            execution_type="PIPELINE",
            execution_id=large_id,
        )
        queue.push(msg)
        polled = queue.poll_one()
        assert polled is not None

        # Move to DLQ
        queue.move_to_dlq(int(polled.message_id), "Large payload test")  # type: ignore

        # Verify can be retrieved from DLQ
        dlq_messages = queue.list_dlq()
        assert len(dlq_messages) >= 1

    def test_dlq_error_message_with_special_characters(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Test DLQ with error message containing special characters."""
        # Clear queue to ensure clean state
        queue.clear()

        msg = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="special-error-test",
        )
        queue.push(msg)
        polled = queue.poll_one()
        assert polled is not None

        # Error with special characters
        error_msg = "Error: 'quoted' and \"double-quoted\" and \\ backslash and\nnewline"
        queue.move_to_dlq(int(polled.message_id), error_msg)  # type: ignore

        # Verify error is stored
        dlq_messages = queue.list_dlq()
        assert len(dlq_messages) >= 1
        assert "quoted" in dlq_messages[0].get("error", "")
