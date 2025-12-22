"""Parameterized tests for Queue - runs on all backends."""

from datetime import timedelta

from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CompleteTask, StartStage, StartWorkflow
from stabilize.queue.queue import Queue


class TestQueue:
    """Parameterized queue tests - runs on both SQLite and PostgreSQL."""

    def test_push_and_poll(self, queue: Queue) -> None:
        """Test pushing and polling a message."""
        message = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="test-123",
        )

        queue.push(message)
        assert queue.size() == 1

        polled = queue.poll_one()
        assert polled is not None
        assert isinstance(polled, StartWorkflow)
        assert polled.execution_id == "test-123"

        queue.ack(polled)
        assert queue.size() == 0

    def test_push_with_delay(self, queue: Queue) -> None:
        """Test pushing a message with delay."""
        message = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="delayed-123",
        )

        # Push with 1 hour delay - should not be immediately available
        queue.push(message, delay=timedelta(hours=1))
        assert queue.size() == 1

        # Should not be polled (not yet delivered)
        polled = queue.poll_one()
        assert polled is None

        # Clear for cleanup
        queue.clear()

    def test_multiple_message_types(self, queue: Queue) -> None:
        """Test different message types."""
        msg1 = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="exec-1",
        )
        msg2 = StartStage(
            execution_type="PIPELINE",
            execution_id="exec-1",
            stage_id="stage-1",
        )
        msg3 = CompleteTask(
            execution_type="PIPELINE",
            execution_id="exec-1",
            stage_id="stage-1",
            task_id="task-1",
            status=WorkflowStatus.SUCCEEDED,
        )

        queue.push(msg1)
        queue.push(msg2)
        queue.push(msg3)

        assert queue.size() == 3

        # Poll and verify types
        polled1 = queue.poll_one()
        assert isinstance(polled1, StartWorkflow)
        queue.ack(polled1)

        polled2 = queue.poll_one()
        assert isinstance(polled2, StartStage)
        queue.ack(polled2)

        polled3 = queue.poll_one()
        assert isinstance(polled3, CompleteTask)
        assert polled3.status == WorkflowStatus.SUCCEEDED
        queue.ack(polled3)

    def test_message_locking(self, queue: Queue) -> None:
        """Test that polling locks messages to prevent double-processing."""
        message = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="lock-test-123",
        )

        queue.push(message)

        # First poll succeeds and locks the message
        polled1 = queue.poll_one()
        assert polled1 is not None
        assert isinstance(polled1, StartWorkflow)
        assert polled1.execution_id == "lock-test-123"

        # Second poll should return None (message is locked)
        polled2 = queue.poll_one()
        assert polled2 is None

        # Ack first message
        queue.ack(polled1)
        assert queue.size() == 0

    def test_reschedule(self, queue: Queue) -> None:
        """Test rescheduling a message."""
        message = StartWorkflow(
            execution_type="PIPELINE",
            execution_id="reschedule-123",
        )

        queue.push(message)
        polled = queue.poll_one()
        assert polled is not None

        # Reschedule with delay
        queue.reschedule(polled, delay=timedelta(hours=1))

        # Message should not be immediately available
        polled2 = queue.poll_one()
        assert polled2 is None

        # But queue size should still be 1
        assert queue.size() == 1

    def test_clear(self, queue: Queue) -> None:
        """Test clearing the queue."""
        for i in range(5):
            queue.push(
                StartWorkflow(
                    execution_type="PIPELINE",
                    execution_id=f"exec-{i}",
                )
            )

        assert queue.size() == 5
        queue.clear()
        assert queue.size() == 0
