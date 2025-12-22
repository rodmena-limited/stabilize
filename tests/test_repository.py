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

    def test_store_execution_with_tasks(self, repository: WorkflowStore) -> None:
        """Test storing execution with tasks."""
        task = TaskExecution.create(
            name="Test Task",
            implementing_class="test.Task",
            stage_start=True,
            stage_end=True,
        )

        stage = StageExecution.create(
            type="test",
            name="Test Stage",
            ref_id="1",
        )
        stage.tasks = [task]

        execution = Workflow.create(
            application="test-app",
            name="Test Pipeline",
            stages=[stage],
        )

        repository.store(execution)
        retrieved = repository.retrieve(execution.id)

        assert len(retrieved.stages) == 1
        assert len(retrieved.stages[0].tasks) == 1
        assert retrieved.stages[0].tasks[0].name == "Test Task"
        assert retrieved.stages[0].tasks[0].stage_start is True
        assert retrieved.stages[0].tasks[0].stage_end is True

        repository.delete(execution.id)

    def test_update_status(self, repository: WorkflowStore) -> None:
        """Test updating execution status."""
        execution = Workflow.create(
            application="test-app",
            name="Test Pipeline",
            stages=[],
        )

        repository.store(execution)

        # Update status
        execution.status = WorkflowStatus.RUNNING
        execution.start_time = 1234567890
        repository.update_status(execution)

        # Verify
        retrieved = repository.retrieve(execution.id)
        assert retrieved.status == WorkflowStatus.RUNNING
        assert retrieved.start_time == 1234567890

        repository.delete(execution.id)

    def test_store_stage(self, repository: WorkflowStore) -> None:
        """Test updating a stage."""
        execution = Workflow.create(
            application="test-app",
            name="Test Pipeline",
            stages=[
                StageExecution.create(
                    type="test",
                    name="Test Stage",
                    ref_id="1",
                ),
            ],
        )

        repository.store(execution)

        # Update stage
        stage = execution.stages[0]
        stage.status = WorkflowStatus.RUNNING
        stage.outputs = {"output": "value"}
        repository.store_stage(stage)

        # Verify
        retrieved = repository.retrieve(execution.id)
        assert retrieved.stages[0].status == WorkflowStatus.RUNNING
        assert retrieved.stages[0].outputs == {"output": "value"}

        repository.delete(execution.id)
