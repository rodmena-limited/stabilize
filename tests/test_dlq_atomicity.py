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
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

import pytest

from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartWorkflow


class _NoCommit:
    """A connection whose commit fails, standing in for a process that dies before it commits."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def commit(self) -> None:
        raise ConnectionError("simulated crash before commit")


@contextmanager
def _crash_before_commit(queue: Any, backend: str) -> Iterator[None]:
    if backend == "postgres":
        engine = queue._engine
        original = engine.connection

        @contextmanager
        def crashing() -> Iterator[Any]:
            with original() as conn:
                yield _NoCommit(conn)

        engine.connection = crashing
        try:
            yield
        finally:
            engine.connection = original
        return
    real = queue._get_connection()
    original_get = queue._get_connection
    queue._get_connection = lambda: _NoCommit(real)
    try:
        yield
    finally:
        queue._get_connection = original_get
        real.rollback()


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

    def test_move_to_dlq_is_atomic_on_crash(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """A crash before the commit leaves the message in the queue and nothing in the DLQ, on both backends."""
        queue = file_queue
        queue.clear()
        queue.clear_dlq()
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01M68T00000000000000000001"))
        polled = queue.poll_one()
        assert polled is not None and polled.message_id is not None
        with _crash_before_commit(queue, backend):
            with pytest.raises(ConnectionError):
                queue.move_to_dlq(polled.message_id, "Simulated crash test")
        assert queue.dlq_size() == 0, "the dead-letter insert survived a crash before commit"
        assert queue.size() == 1, "the queue row was deleted by a transaction that never committed"
        queue.clear()

    def test_concurrent_move_to_dlq_same_message(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """Concurrent move_to_dlq calls: only one succeeds, no duplicates in DLQ.

        Runs on both backends. SQLite's thread-local connections are only a
        problem for a `:memory:` database, which is per-connection; a
        file-backed one is shared, and file_queue supplies it.
        """
        queue = file_queue

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

    def test_replay_dlq_is_atomic_on_crash(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """A crash before the commit of a replay leaves the message in the DLQ and nothing in the queue."""
        queue = file_queue
        queue.clear()
        queue.clear_dlq()
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01M68T00000000000000000002"))
        polled = queue.poll_one()
        assert polled is not None and polled.message_id is not None
        queue.move_to_dlq(polled.message_id, "Initial error")
        [entry] = queue.list_dlq()
        with _crash_before_commit(queue, backend):
            with pytest.raises(ConnectionError):
                queue.replay_dlq(entry["id"])
        assert queue.dlq_size() == 1, "the dead-letter row was deleted by a replay that never committed"
        assert queue.size() == 0, "the replayed row survived a crash before commit"
        queue.clear_dlq()

    def test_dlq_operations_under_load(self, file_repository: WorkflowStore, file_queue: Queue, backend: str) -> None:
        """
        Stress test DLQ operations with concurrent message processing failures.

        Push many messages, have them all fail, and verify DLQ is consistent.

        Runs on BOTH backends. The previous skip claimed "SQLite doesn't handle
        high-concurrency DLQ operations reliably", which was never measured and
        was not the reason: the regular sqlite fixture is `:memory:`, which is
        per-connection, so ten worker threads addressed ten separate databases.
        A file-backed database is what the concurrency here requires, and the
        file_queue fixture already provides one.
        """
        queue = file_queue

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
