"""
Tests for synthetic stage (setup/teardown) coordination edge cases.

This module tests edge cases in synthetic stage handling including:
- STAGE_BEFORE must complete before main stage tasks start
- STAGE_AFTER runs after main stage completes
- Synthetic stages reset when parent is jumped to
- Orphan synthetic stages (no parent_stage_id)
"""



from stabilize import TaskResult
from stabilize.models.stage import StageExecution, SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize


class OrderTracker(Task):
    """Task that tracks execution order."""

    execution_order: list = []

    def execute(self, stage: StageExecution) -> TaskResult:
        OrderTracker.execution_order.append(
            f"{stage.ref_id}:{stage.synthetic_stage_owner.name if stage.synthetic_stage_owner else 'MAIN'}"
        )
        return TaskResult.success()


class SleepTask(Task):
    """Task that sleeps to ensure ordering."""

    sleep_time: float = 0.1

    def execute(self, stage: StageExecution) -> TaskResult:
        import time

        time.sleep(SleepTask.sleep_time)
        OrderTracker.execution_order.append(
            f"{stage.ref_id}:{stage.synthetic_stage_owner.name if stage.synthetic_stage_owner else 'MAIN'}"
        )
        return TaskResult.success()


class ConditionalJumpTask(Task):
    """Task that jumps once, then succeeds."""

    jumped: bool = False

    def execute(self, stage: StageExecution) -> TaskResult:
        if not ConditionalJumpTask.jumped:
            ConditionalJumpTask.jumped = True
            return TaskResult.jump_to("parent_stage")
        return TaskResult.success()


def reset_tasks() -> None:
    """Reset all task class state."""
    OrderTracker.execution_order = []
    SleepTask.sleep_time = 0.1
    ConditionalJumpTask.jumped = False


class TestSyntheticStageOrdering:
    """Test synthetic stage execution ordering."""

    def test_before_stage_runs_before_main_tasks(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that STAGE_BEFORE synthetic stage completes before main stage tasks start.
        """
        reset_tasks()

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"order": OrderTracker, "sleep": SleepTask}
        )

        # Create parent stage with synthetic before-stage
        parent_stage = StageExecution(
            ref_id="parent_stage",
            name="Parent Stage",
            tasks=[
                TaskExecution.create(
                    name="Main Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        # Create synthetic before-stage
        before_stage = StageExecution(
            ref_id="before_stage",
            name="Before Stage",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
            tasks=[
                TaskExecution.create(
                    name="Before Task",
                    implementing_class="sleep",  # Sleep to ensure ordering
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        execution = Workflow.create(
            application="test",
            name="Before Stage Test",
            stages=[parent_stage, before_stage],
        )

        # Link stages
        before_stage.parent_stage_id = parent_stage.id

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        # Before stage should execute before main
        assert len(OrderTracker.execution_order) >= 2
        before_idx = next(
            (i for i, x in enumerate(OrderTracker.execution_order) if "STAGE_BEFORE" in x),
            -1,
        )
        main_idx = next(
            (i for i, x in enumerate(OrderTracker.execution_order) if "MAIN" in x),
            -1,
        )

        if before_idx >= 0 and main_idx >= 0:
            assert before_idx < main_idx, f"Before stage should run first. Order: {OrderTracker.execution_order}"

    def test_after_stage_runs_after_main_tasks(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that STAGE_AFTER synthetic stage runs after main stage tasks complete.
        """
        reset_tasks()

        processor, runner, _ = setup_stabilize(
            repository, queue, extra_tasks={"order": OrderTracker, "sleep": SleepTask}
        )

        # Create parent stage with synthetic after-stage
        parent_stage = StageExecution(
            ref_id="parent_stage",
            name="Parent Stage",
            tasks=[
                TaskExecution.create(
                    name="Main Task",
                    implementing_class="sleep",  # Sleep to ensure ordering
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        # Create synthetic after-stage
        after_stage = StageExecution(
            ref_id="after_stage",
            name="After Stage",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_AFTER,
            tasks=[
                TaskExecution.create(
                    name="After Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        execution = Workflow.create(
            application="test",
            name="After Stage Test",
            stages=[parent_stage, after_stage],
        )

        after_stage.parent_stage_id = parent_stage.id

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        # After stage should execute after main
        assert len(OrderTracker.execution_order) >= 2
        main_idx = next(
            (i for i, x in enumerate(OrderTracker.execution_order) if "MAIN" in x),
            -1,
        )
        after_idx = next(
            (i for i, x in enumerate(OrderTracker.execution_order) if "STAGE_AFTER" in x),
            -1,
        )

        if main_idx >= 0 and after_idx >= 0:
            assert main_idx < after_idx, f"Main should run before after stage. Order: {OrderTracker.execution_order}"


class TestSyntheticStageResetOnJump:
    """Test that synthetic stages are reset when parent is jumped to."""

    def test_synthetic_reset_on_backward_jump(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        Test that jumping back to a stage resets its synthetic stages too.
        """
        reset_tasks()

        class CountingSetupTask(Task):
            count: int = 0

            def execute(self, stage: StageExecution) -> TaskResult:
                CountingSetupTask.count += 1
                return TaskResult.success()

        CountingSetupTask.count = 0

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={
                "counting_setup": CountingSetupTask,
                "cond_jump": ConditionalJumpTask,
                "order": OrderTracker,
            },
        )

        # Parent stage with before synthetic
        parent_stage = StageExecution(
            ref_id="parent_stage",
            name="Parent Stage",
            tasks=[
                TaskExecution.create(
                    name="Main Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        before_stage = StageExecution(
            ref_id="before_stage",
            name="Setup Stage",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
            tasks=[
                TaskExecution.create(
                    name="Setup Task",
                    implementing_class="counting_setup",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        # Next stage that jumps back
        next_stage = StageExecution(
            ref_id="next_stage",
            name="Next Stage",
            requisite_stage_ref_ids={"parent_stage"},
            tasks=[
                TaskExecution.create(
                    name="Jump Task",
                    implementing_class="cond_jump",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        execution = Workflow.create(
            application="test",
            name="Synthetic Reset Test",
            stages=[parent_stage, before_stage, next_stage],
        )

        before_stage.parent_stage_id = parent_stage.id

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        # Setup task should have run twice (once per parent execution)
        assert CountingSetupTask.count == 2, f"Setup should run twice after jump. Got {CountingSetupTask.count}"


class TestSyntheticStageEdgeCases:
    """Edge cases for synthetic stages."""

    def test_multiple_before_stages(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test handling of multiple STAGE_BEFORE stages for same parent."""
        reset_tasks()

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"order": OrderTracker})

        parent_stage = StageExecution(
            ref_id="parent_stage",
            name="Parent Stage",
            tasks=[
                TaskExecution.create(
                    name="Main Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        before_1 = StageExecution(
            ref_id="before_1",
            name="Before 1",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
            tasks=[
                TaskExecution.create(
                    name="Before 1 Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        before_2 = StageExecution(
            ref_id="before_2",
            name="Before 2",
            requisite_stage_ref_ids={"before_1"},  # Depends on before_1
            synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
            tasks=[
                TaskExecution.create(
                    name="Before 2 Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        execution = Workflow.create(
            application="test",
            name="Multiple Before Test",
            stages=[parent_stage, before_1, before_2],
        )

        before_1.parent_stage_id = parent_stage.id
        before_2.parent_stage_id = parent_stage.id

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        # All three should have executed
        assert len(OrderTracker.execution_order) >= 3

    def test_nested_synthetic_stages(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test parent with both before AND after synthetic stages."""
        reset_tasks()

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"order": OrderTracker})

        parent_stage = StageExecution(
            ref_id="parent_stage",
            name="Parent Stage",
            tasks=[
                TaskExecution.create(
                    name="Main Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        before_stage = StageExecution(
            ref_id="before_stage",
            name="Before Stage",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
            tasks=[
                TaskExecution.create(
                    name="Before Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        after_stage = StageExecution(
            ref_id="after_stage",
            name="After Stage",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_AFTER,
            tasks=[
                TaskExecution.create(
                    name="After Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        execution = Workflow.create(
            application="test",
            name="Both Synthetic Test",
            stages=[parent_stage, before_stage, after_stage],
        )

        before_stage.parent_stage_id = parent_stage.id
        after_stage.parent_stage_id = parent_stage.id

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        # All three should have executed in order: before, main, after
        assert len(OrderTracker.execution_order) >= 3


class TestSyntheticStageFailure:
    """Test synthetic stage failure handling."""

    def test_before_stage_failure_prevents_main(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """Test that if STAGE_BEFORE fails, main stage doesn't run."""
        reset_tasks()

        class FailTask(Task):
            def execute(self, stage: StageExecution) -> TaskResult:
                return TaskResult.terminal("Intentional failure")

        processor, runner, _ = setup_stabilize(repository, queue, extra_tasks={"fail": FailTask, "order": OrderTracker})

        parent_stage = StageExecution(
            ref_id="parent_stage",
            name="Parent Stage",
            tasks=[
                TaskExecution.create(
                    name="Main Task",
                    implementing_class="order",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        before_stage = StageExecution(
            ref_id="before_stage",
            name="Before Stage",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
            tasks=[
                TaskExecution.create(
                    name="Failing Before Task",
                    implementing_class="fail",
                    stage_start=True,
                    stage_end=True,
                )
            ],
        )

        execution = Workflow.create(
            application="test",
            name="Before Failure Test",
            stages=[parent_stage, before_stage],
        )

        before_stage.parent_stage_id = parent_stage.id

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)

        # Workflow should be TERMINAL due to before stage failure
        assert result.status == WorkflowStatus.TERMINAL, f"Status: {result.status}"

        # Main task should NOT have executed
        main_executed = any("MAIN" in x for x in OrderTracker.execution_order)
        assert not main_executed, "Main task should not execute if before stage fails"
