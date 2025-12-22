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
