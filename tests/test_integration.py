from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue
from tests.conftest import CounterTask, setup_stabilize

class TestPipelineIntegration:
    """Parameterized integration tests - runs on both SQLite and PostgreSQL."""

    def test_simple_pipeline(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test a simple pipeline with one stage."""
        processor, runner = setup_stabilize(repository, queue)

        execution = Workflow.create(
            application="test",
            name="Simple Pipeline",
            stages=[
                StageExecution(
                    ref_id="1",
                    type="test",
                    name="Test Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Success Task",
                            implementing_class="success",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)

        # Process all messages
        processor.process_all(timeout=10.0)

        # Verify
        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.stages[0].status == WorkflowStatus.SUCCEEDED

        # Cleanup
        repository.delete(execution.id)
