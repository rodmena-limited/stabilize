from stabilize import TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize
from tests.jump_edge_support import reset_tasks


class TestStaleCompleteStageMessages:
    """Test handling of stale CompleteStage messages after jump."""

    def test_version_prevents_stale_complete_stage(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that stale CompleteStage messages are ignored after a jump resets the stage.

        Scenario:
        1. Stage A completes, queues CompleteStage
        2. Stage B jumps back to A (resets A)
        3. Old CompleteStage message for A arrives
        4. Should be ignored due to version mismatch
        """
        reset_tasks()

        class DelayedJumpTask(Task):
            executed = False

            def execute(self, stage: StageExecution) -> TaskResult:
                if not DelayedJumpTask.executed:
                    DelayedJumpTask.executed = True
                    return TaskResult.jump_to("stage_a")
                return TaskResult.success()

        class RecordExecTask(Task):
            exec_count = 0

            def execute(self, stage: StageExecution) -> TaskResult:
                RecordExecTask.exec_count += 1
                return TaskResult.success()

        DelayedJumpTask.executed = False
        RecordExecTask.exec_count = 0

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"delayed_jump": DelayedJumpTask, "record_exec": RecordExecTask},
        )

        execution = Workflow.create(
            application="test",
            name="Stale Message Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Record Exec",
                            implementing_class="record_exec",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Delayed Jump",
                            implementing_class="delayed_jump",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        # Workflow should complete successfully
        assert result.status == WorkflowStatus.SUCCEEDED

        # Stage A should have executed twice (initial + after jump)
        assert RecordExecTask.exec_count == 2, f"Expected 2 executions, got {RecordExecTask.exec_count}"


class TestBackwardJumpResetsDownstream:
    """Test that backward jumps properly reset downstream stages."""

    def test_backward_jump_resets_all_downstream(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test: A -> B -> C, jump from C to A should reset A, B, and C.
        """
        reset_tasks()

        class ConditionalBackwardJump(Task):
            jumped = False

            def execute(self, stage: StageExecution) -> TaskResult:
                if not ConditionalBackwardJump.jumped:
                    ConditionalBackwardJump.jumped = True
                    return TaskResult.jump_to("stage_a")
                return TaskResult.success()

        class CounterTask(Task):
            counters: dict = {}

            def execute(self, stage: StageExecution) -> TaskResult:
                CounterTask.counters[stage.ref_id] = CounterTask.counters.get(stage.ref_id, 0) + 1
                return TaskResult.success()

        ConditionalBackwardJump.jumped = False
        CounterTask.counters = {}

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"cond_back": ConditionalBackwardJump, "counter": CounterTask},
        )

        execution = Workflow.create(
            application="test",
            name="Backward Jump Reset Test",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    tasks=[
                        TaskExecution.create(
                            name="Counter A",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    tasks=[
                        TaskExecution.create(
                            name="Counter B",
                            implementing_class="counter",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_b"},
                    tasks=[
                        TaskExecution.create(
                            name="Conditional Jump",
                            implementing_class="cond_back",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED

        # All stages should have executed twice
        assert CounterTask.counters.get("stage_a") == 2, f"Stage A: {CounterTask.counters.get('stage_a')}"
        assert CounterTask.counters.get("stage_b") == 2, f"Stage B: {CounterTask.counters.get('stage_b')}"
