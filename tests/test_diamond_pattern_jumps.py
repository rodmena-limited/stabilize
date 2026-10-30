from __future__ import annotations

import random
import threading

import pytest

from stabilize import (
    StageExecution,
    Task,
    TaskResult,
)
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.diamond_support import OutputTask, VerifyInputsTask


class TestDiamondWithJumps:
    """
    Test diamond patterns combined with dynamic routing (jumps).

    This is the most complex scenario: a diamond pattern where one
    of the branches triggers a jump back to retry the diamond.
    """

    def test_diamond_with_conditional_retry(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Test diamond where branch C can trigger a retry of the entire diamond.

        First iteration: C jumps back to A (retry)
        Second iteration: C succeeds

        This tests:
        - Proper reset of all stages on jump
        - Context preservation across iterations
        - No duplicate execution of join point D
        """

        class JumpOnFirstAttemptTask(Task):
            """Task that jumps back on first attempt, succeeds on second."""

            attempts: dict[str, int] = {}
            _lock = threading.Lock()

            @classmethod
            def reset(cls) -> None:
                with cls._lock:
                    cls.attempts = {}

            def execute(self, stage: StageExecution) -> TaskResult:
                key = f"{stage.execution.id}:{stage.ref_id}"
                with JumpOnFirstAttemptTask._lock:
                    attempt = JumpOnFirstAttemptTask.attempts.get(key, 0) + 1
                    JumpOnFirstAttemptTask.attempts[key] = attempt

                if attempt == 1:
                    # First attempt - jump back to A
                    return TaskResult.jump_to(
                        target_stage_ref_id="stage_a",
                        context={"retry_from_c": True, "attempt": attempt},
                        outputs={"jump_triggered": True},
                    )
                else:
                    # Second attempt - succeed
                    return TaskResult.success(outputs={"from_c": True, "attempt": attempt})

        JumpOnFirstAttemptTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "jump_first": JumpOnFirstAttemptTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        wf = Workflow.create(
            application="diamond-jump-test",
            name="conditional-retry",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_b": True}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C (May Jump)",
                    requisite_stage_ref_ids={"stage_a"},
                    context={},  # Uses JumpOnFirstAttemptTask
                    tasks=[TaskExecution.create("Task C", "jump_first", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["from_a", "from_b", "from_c"]},
                    tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"
        assert not VerifyInputsTask.verification_errors

        # Verify C was executed twice (jump + retry)
        c_key = f"{wf.id}:stage_c"
        assert JumpOnFirstAttemptTask.attempts.get(c_key, 0) == 2, (
            f"C should have been executed twice, got {JumpOnFirstAttemptTask.attempts.get(c_key, 0)}"
        )

    def test_diamond_jump_from_join_point(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Test jump from the join point D back to one of the branches.

        This is tricky because D has received outputs from both B and C,
        but on retry, those outputs should be regenerated.
        """

        class JumpFromJoinTask(Task):
            """Task that jumps back from join point on first attempt."""

            attempts: dict[str, int] = {}
            _lock = threading.Lock()

            @classmethod
            def reset(cls) -> None:
                with cls._lock:
                    cls.attempts = {}

            def execute(self, stage: StageExecution) -> TaskResult:
                key = f"{stage.execution.id}:{stage.ref_id}"
                expected = stage.context.get("expected_keys", [])

                # Verify we have all expected inputs
                missing = [k for k in expected if k not in stage.context]
                if missing:
                    return TaskResult.terminal(f"Missing keys at join: {missing}")

                with JumpFromJoinTask._lock:
                    attempt = JumpFromJoinTask.attempts.get(key, 0) + 1
                    JumpFromJoinTask.attempts[key] = attempt

                if attempt == 1:
                    # First attempt at join - jump back to B
                    return TaskResult.jump_to(
                        target_stage_ref_id="stage_b",
                        context={"retry_from_d": True},
                        outputs={"d_jump_triggered": True},
                    )
                else:
                    # Second attempt - succeed
                    return TaskResult.success(outputs={"from_d": True, "d_attempts": attempt})

        JumpFromJoinTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "output": OutputTask,
                "jump_from_join": JumpFromJoinTask,
            },
        )

        wf = Workflow.create(
            application="diamond-jump-test",
            name="jump-from-join",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    name="Stage A",
                    context={"outputs_to_produce": {"from_a": True}},
                    tasks=[TaskExecution.create("Task A", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    name="Stage B",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_b": True}},
                    tasks=[TaskExecution.create("Task B", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_c",
                    name="Stage C",
                    requisite_stage_ref_ids={"stage_a"},
                    context={"outputs_to_produce": {"from_c": True}},
                    tasks=[TaskExecution.create("Task C", "output", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_d",
                    name="Stage D (Join - May Jump)",
                    requisite_stage_ref_ids={"stage_b", "stage_c"},
                    context={"expected_keys": ["from_a", "from_b", "from_c"]},
                    tasks=[TaskExecution.create("Task D", "jump_from_join", stage_start=True, stage_end=True)],
                ),
            ],
        )

        file_repository.store(wf)
        runner.start(wf)
        processor.process_all(timeout=60.0)

        result = file_repository.retrieve(wf.id)
        assert result.status == WorkflowStatus.SUCCEEDED, f"Status: {result.status}"

        # Verify D was executed twice
        d_key = f"{wf.id}:stage_d"
        assert JumpFromJoinTask.attempts.get(d_key, 0) == 2


class TestDiamondChaos:
    """
    Chaos testing for diamond patterns - random failures and jumps.

    These tests are designed to expose any hidden bugs by creating
    unpredictable execution patterns.
    """

    @pytest.mark.stress
    def test_many_diamonds_with_random_jumps(
        self, file_repository: WorkflowStore, file_queue: Queue, backend: str
    ) -> None:
        """
        Run many diamonds where branches randomly decide to jump.

        This creates maximum chaos to expose race conditions.
        """

        class MaybeJumpTask(Task):
            """Task that may randomly trigger a jump."""

            _lock = threading.Lock()
            jump_count: int = 0
            success_count: int = 0

            @classmethod
            def reset(cls) -> None:
                with cls._lock:
                    cls.jump_count = 0
                    cls.success_count = 0

            def execute(self, stage: StageExecution) -> TaskResult:
                # 20% chance to jump back to A on first iteration
                iteration = stage.context.get("_jump_iteration", 0)

                if iteration == 0 and random.random() < 0.2:
                    with MaybeJumpTask._lock:
                        MaybeJumpTask.jump_count += 1
                    return TaskResult.jump_to(
                        target_stage_ref_id="stage_a",
                        context={"_jump_iteration": iteration + 1},
                        outputs={"jumped": True},
                    )

                with MaybeJumpTask._lock:
                    MaybeJumpTask.success_count += 1

                outputs = stage.context.get("outputs_to_produce", {})
                return TaskResult.success(outputs=outputs)

        MaybeJumpTask.reset()
        VerifyInputsTask.reset()

        processor, runner, _ = setup_stabilize(
            file_repository,
            file_queue,
            extra_tasks={
                "maybe_jump": MaybeJumpTask,
                "verify_inputs": VerifyInputsTask,
            },
        )

        num_workflows = 20
        workflow_ids = []

        for i in range(num_workflows):
            wf = Workflow.create(
                application="chaos-test",
                name=f"chaos-diamond-{i}",
                stages=[
                    StageExecution(
                        ref_id="stage_a",
                        name="Stage A",
                        context={"outputs_to_produce": {"from_a": True, "wf_index": i}},
                        tasks=[TaskExecution.create("Task A", "maybe_jump", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_b",
                        name="Stage B",
                        requisite_stage_ref_ids={"stage_a"},
                        context={"outputs_to_produce": {"from_b": True}},
                        tasks=[TaskExecution.create("Task B", "maybe_jump", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_c",
                        name="Stage C",
                        requisite_stage_ref_ids={"stage_a"},
                        context={"outputs_to_produce": {"from_c": True}},
                        tasks=[TaskExecution.create("Task C", "maybe_jump", stage_start=True, stage_end=True)],
                    ),
                    StageExecution(
                        ref_id="stage_d",
                        name="Stage D",
                        requisite_stage_ref_ids={"stage_b", "stage_c"},
                        context={"expected_keys": ["from_a", "from_b", "from_c", "wf_index"]},
                        tasks=[TaskExecution.create("Task D", "verify_inputs", stage_start=True, stage_end=True)],
                    ),
                ],
            )

            file_repository.store(wf)
            runner.start(wf)
            workflow_ids.append(wf.id)

        processor.process_all(timeout=180.0)

        # All workflows should eventually succeed despite random jumps
        failed = []
        for wf_id in workflow_ids:
            result = file_repository.retrieve(wf_id)
            if result.status != WorkflowStatus.SUCCEEDED:
                failed.append((wf_id, result.status))

        assert not failed, f"Failed workflows: {failed}"
        assert not VerifyInputsTask.verification_errors, f"Errors: {VerifyInputsTask.verification_errors}"

        # Log chaos statistics
        print(f"\nChaos stats: {MaybeJumpTask.jump_count} jumps, {MaybeJumpTask.success_count} successes")
