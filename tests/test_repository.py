"""Parameterized tests for WorkflowStore - runs on all backends."""

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

    def test_cancel_execution(self, repository: WorkflowStore) -> None:
        """Test canceling an execution."""
        execution = Workflow.create(
            application="test-app",
            name="Test Pipeline",
            stages=[],
        )

        repository.store(execution)
        repository.cancel(execution.id, "user@test.com", "Testing cancellation")

        retrieved = repository.retrieve(execution.id)
        assert retrieved.is_canceled is True
        assert retrieved.canceled_by == "user@test.com"
        assert retrieved.cancellation_reason == "Testing cancellation"

        repository.delete(execution.id)

    def test_pause_and_resume(self, repository: WorkflowStore) -> None:
        """Test pausing and resuming an execution."""
        execution = Workflow.create(
            application="test-app",
            name="Test Pipeline",
            stages=[],
        )

        repository.store(execution)

        # Pause
        repository.pause(execution.id, "admin@test.com")
        retrieved = repository.retrieve(execution.id)
        assert retrieved.status == WorkflowStatus.PAUSED
        assert retrieved.paused is not None
        assert retrieved.paused.paused_by == "admin@test.com"

        # Resume
        repository.resume(execution.id)
        retrieved = repository.retrieve(execution.id)
        assert retrieved.status == WorkflowStatus.RUNNING

        repository.delete(execution.id)

    def test_retrieve_by_application(self, repository: WorkflowStore) -> None:
        """Test retrieving executions by application."""
        # Create executions
        exec1 = Workflow.create(
            application="app-a",
            name="Pipeline 1",
            stages=[],
        )
        exec2 = Workflow.create(
            application="app-a",
            name="Pipeline 2",
            stages=[],
        )
        exec3 = Workflow.create(
            application="app-b",
            name="Pipeline 3",
            stages=[],
        )

        repository.store(exec1)
        repository.store(exec2)
        repository.store(exec3)

        # Retrieve by application
        results = list(repository.retrieve_by_application("app-a"))
        assert len(results) == 2
        assert all(e.application == "app-a" for e in results)

        # Cleanup
        repository.delete(exec1.id)
        repository.delete(exec2.id)
        repository.delete(exec3.id)

    def test_dag_with_requisites(self, repository: WorkflowStore) -> None:
        """Test storing execution with DAG dependencies."""
        execution = Workflow.create(
            application="test-app",
            name="DAG Pipeline",
            stages=[
                StageExecution.create(
                    type="test",
                    name="Stage A",
                    ref_id="a",
                ),
                StageExecution.create(
                    type="test",
                    name="Stage B",
                    ref_id="b",
                    requisite_stage_ref_ids={"a"},
                ),
                StageExecution.create(
                    type="test",
                    name="Stage C",
                    ref_id="c",
                    requisite_stage_ref_ids={"a"},
                ),
                StageExecution.create(
                    type="test",
                    name="Stage D",
                    ref_id="d",
                    requisite_stage_ref_ids={"b", "c"},
                ),
            ],
        )

        repository.store(execution)
        retrieved = repository.retrieve(execution.id)

        # Verify DAG structure
        stage_map = {s.ref_id: s for s in retrieved.stages}
        assert stage_map["a"].requisite_stage_ref_ids == set()
        assert stage_map["b"].requisite_stage_ref_ids == {"a"}
        assert stage_map["c"].requisite_stage_ref_ids == {"a"}
        assert stage_map["d"].requisite_stage_ref_ids == {"b", "c"}

        repository.delete(execution.id)

    def test_json_context_roundtrip(self, repository: WorkflowStore) -> None:
        """Test that complex JSON context is preserved."""
        execution = Workflow.create(
            application="test-app",
            name="JSON Test",
            stages=[
                StageExecution.create(
                    type="test",
                    name="Test Stage",
                    ref_id="1",
                    context={
                        "string": "value",
                        "number": 42,
                        "float": 3.14,
                        "boolean": True,
                        "null": None,
                        "array": [1, 2, 3],
                        "nested": {"a": {"b": {"c": "deep"}}},
                    },
                ),
            ],
        )

        repository.store(execution)
        retrieved = repository.retrieve(execution.id)

        ctx = retrieved.stages[0].context
        assert ctx["string"] == "value"
        assert ctx["number"] == 42
        assert ctx["float"] == 3.14
        assert ctx["boolean"] is True
        assert ctx["null"] is None
        assert ctx["array"] == [1, 2, 3]
        assert ctx["nested"]["a"]["b"]["c"] == "deep"

        repository.delete(execution.id)
