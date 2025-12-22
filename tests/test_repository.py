from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore

class TestWorkflowStore:
    """Parameterized repository tests - runs on both SQLite and PostgreSQL."""

    def test_health_check(self, repository: WorkflowStore) -> None:
        """Test database health check."""
        assert repository.is_healthy() is True

    def test_store_and_retrieve_execution(self, repository: WorkflowStore) -> None:
        """Test storing and retrieving a pipeline execution."""
        execution = Workflow.create(
            application="test-app",
            name="Test Pipeline",
            stages=[
                StageExecution.create(
                    type="test",
                    name="Test Stage",
                    ref_id="1",
                    context={"key": "value"},
                ),
            ],
        )

        # Store
        repository.store(execution)

        # Retrieve
        retrieved = repository.retrieve(execution.id)

        assert retrieved.id == execution.id
        assert retrieved.application == "test-app"
        assert retrieved.name == "Test Pipeline"
        assert retrieved.status == WorkflowStatus.NOT_STARTED
        assert len(retrieved.stages) == 1
        assert retrieved.stages[0].ref_id == "1"
        assert retrieved.stages[0].context == {"key": "value"}

        # Cleanup
        repository.delete(execution.id)
