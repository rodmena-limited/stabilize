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
