"""Parameterized integration tests - runs on all backends."""

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

# Import from conftest - pytest makes these available
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

    def test_linear_pipeline(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test a linear pipeline: A -> B -> C."""
        processor, runner = setup_stabilize(repository, queue)
        CounterTask.counter = 0

        execution = Workflow.create(
            application="test",
            name="Linear Pipeline",
            stages=[
                StageExecution(
                    ref_id="1",
                    type="test",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Counter Task",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="2",
                    type="test",
                    name="Stage B",
                    requisite_stage_ref_ids={"1"},
                    tasks=[
                        TaskExecution.create(
                            name="Counter Task",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="3",
                    type="test",
                    name="Stage C",
                    requisite_stage_ref_ids={"2"},
                    tasks=[
                        TaskExecution.create(
                            name="Counter Task",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert all(s.status == WorkflowStatus.SUCCEEDED for s in result.stages)
        assert CounterTask.counter == 3

        repository.delete(execution.id)

    def test_parallel_stages(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test parallel execution: A -> [B, C] -> D."""
        processor, runner = setup_stabilize(repository, queue)
        CounterTask.counter = 0

        execution = Workflow.create(
            application="test",
            name="Parallel Pipeline",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="test",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Counter Task",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="b",
                    type="test",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                    tasks=[
                        TaskExecution.create(
                            name="Counter Task",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="c",
                    type="test",
                    name="Stage C",
                    requisite_stage_ref_ids={"a"},
                    tasks=[
                        TaskExecution.create(
                            name="Counter Task",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="d",
                    type="test",
                    name="Stage D",
                    requisite_stage_ref_ids={"b", "c"},
                    tasks=[
                        TaskExecution.create(
                            name="Counter Task",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert all(s.status == WorkflowStatus.SUCCEEDED for s in result.stages)
        assert CounterTask.counter == 4

        repository.delete(execution.id)

    def test_stage_failure(
        self,
        repository: WorkflowStore,
        queue: Queue,
    ) -> None:
        """Test that stage failure terminates the pipeline."""
        processor, runner = setup_stabilize(repository, queue)

        execution = Workflow.create(
            application="test",
            name="Failing Pipeline",
            stages=[
                StageExecution(
                    ref_id="1",
                    type="test",
                    name="Failing Stage",
                    tasks=[
                        TaskExecution.create(
                            name="Fail Task",
                            implementing_class="fail",
                            stage_start=True,
                            stage_end=True,
                        ),
                    ],
                ),
                StageExecution(
                    ref_id="2",
                    type="test",
                    name="Never Reached",
                    requisite_stage_ref_ids={"1"},
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
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.TERMINAL

        # Find stages by ref_id (order may vary from DB)
        stage_map = {s.ref_id: s for s in result.stages}
        assert stage_map["1"].status == WorkflowStatus.TERMINAL
        assert stage_map["2"].status == WorkflowStatus.NOT_STARTED

        repository.delete(execution.id)
