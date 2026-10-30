"""Tests for fan-in-aware jump traversal and backward jump with fan-in DAGs.

Covers the critical bug where backward jumps incorrectly reset fan-in stages
that have upstream dependencies from branches not involved in the jump.
"""

from stabilize import TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize


class CounterTask(Task):
    counters: dict[str, int] = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        CounterTask.counters[stage.ref_id] = CounterTask.counters.get(stage.ref_id, 0) + 1
        return TaskResult.success(outputs={"count": CounterTask.counters[stage.ref_id]})


class ConditionalJumpTask(Task):
    jumped: bool = False

    def execute(self, stage: StageExecution) -> TaskResult:
        ConditionalJumpTask.counters_ref = stage.ref_id
        if not ConditionalJumpTask.jumped:
            ConditionalJumpTask.jumped = True
            target = stage.context.get("jump_target", "")
            return TaskResult.jump_to(target)
        return TaskResult.success()


def _reset_test_tasks() -> None:
    CounterTask.counters = {}
    ConditionalJumpTask.jumped = False


def _make_task(impl: str) -> list[TaskExecution]:
    return [TaskExecution.create(name=impl, implementing_class=impl, stage_start=True, stage_end=True)]


class TestBackwardJumpWithFanIn:
    """Integration tests: backward jump should not reset fan-in stages with external upstreams."""

    def test_fan_in_stage_not_reset_on_backward_jump(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """
        DAG:
            a1    a2
            |     |
            b1    b2 (jumps back to a2 once)
             \\   /
            fan_in -> final

        Jump from b2 to a2. b1 is NOT in the reset scope.
        fan_in should NOT be reset (it depends on b1 which is outside scope).
        fan_in should run exactly once (after both b1 and b2 complete).
        """
        _reset_test_tasks()

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"counter": CounterTask, "cond_jump": ConditionalJumpTask},
        )

        execution = Workflow.create(
            application="test",
            name="Fan-In Backward Jump",
            stages=[
                StageExecution(ref_id="a1", tasks=_make_task("counter")),
                StageExecution(ref_id="a2", tasks=_make_task("counter")),
                StageExecution(ref_id="b1", requisite_stage_ref_ids={"a1"}, tasks=_make_task("counter")),
                StageExecution(
                    ref_id="b2",
                    requisite_stage_ref_ids={"a2"},
                    context={"jump_target": "a2", "_max_jumps": 2},
                    tasks=_make_task("cond_jump"),
                ),
                StageExecution(
                    ref_id="fan_in",
                    requisite_stage_ref_ids={"b1", "b2"},
                    tasks=_make_task("counter"),
                ),
                StageExecution(
                    ref_id="final",
                    requisite_stage_ref_ids={"fan_in"},
                    tasks=_make_task("counter"),
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, (
            f"Workflow status: {result.status}. Stage statuses: {[(s.ref_id, s.status.name) for s in result.stages]}"
        )

        fan_in = result.stage_by_ref_id("fan_in")
        assert fan_in is not None
        assert fan_in.status == WorkflowStatus.SUCCEEDED

        final = result.stage_by_ref_id("final")
        assert final is not None
        assert final.status == WorkflowStatus.SUCCEEDED

        assert CounterTask.counters.get("a1") == 1
        assert CounterTask.counters.get("a2") == 2
        assert CounterTask.counters.get("b1") == 1
        assert CounterTask.counters.get("fan_in") == 1
        assert CounterTask.counters.get("final") == 1

    def test_four_branch_fan_in_one_branch_retries(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """
        The exact DAG from the bug report:

        validate_errors  validate_backoff  validate_utils  validate_state
             |                |                 |               |
         repair_errors   repair_backoff    repair_utils    repair_state
                              ↑ jumps to validate_backoff once
              \\               |                /               /
               +---------write_tests_core---------+
                              |
                        implement_core

        Only repair_backoff retries. write_tests_core and implement_core should
        NOT be reset/skipped — they should wait and run once after all repairs complete.
        """
        _reset_test_tasks()

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"counter": CounterTask, "cond_jump": ConditionalJumpTask},
        )

        execution = Workflow.create(
            application="test",
            name="Bug Report DAG",
            stages=[
                StageExecution(ref_id="validate_errors", tasks=_make_task("counter")),
                StageExecution(ref_id="validate_backoff", tasks=_make_task("counter")),
                StageExecution(ref_id="validate_utils", tasks=_make_task("counter")),
                StageExecution(ref_id="validate_state", tasks=_make_task("counter")),
                StageExecution(
                    ref_id="repair_errors",
                    requisite_stage_ref_ids={"validate_errors"},
                    tasks=_make_task("counter"),
                ),
                StageExecution(
                    ref_id="repair_backoff",
                    requisite_stage_ref_ids={"validate_backoff"},
                    context={"jump_target": "validate_backoff", "_max_jumps": 2},
                    tasks=_make_task("cond_jump"),
                ),
                StageExecution(
                    ref_id="repair_utils",
                    requisite_stage_ref_ids={"validate_utils"},
                    tasks=_make_task("counter"),
                ),
                StageExecution(
                    ref_id="repair_state",
                    requisite_stage_ref_ids={"validate_state"},
                    tasks=_make_task("counter"),
                ),
                StageExecution(
                    ref_id="write_tests_core",
                    requisite_stage_ref_ids={"repair_errors", "repair_backoff", "repair_utils", "repair_state"},
                    tasks=_make_task("counter"),
                ),
                StageExecution(
                    ref_id="implement_core",
                    requisite_stage_ref_ids={"write_tests_core"},
                    tasks=_make_task("counter"),
                ),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=20.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED, (
            f"Workflow status: {result.status}. Stage statuses: {[(s.ref_id, s.status.name) for s in result.stages]}"
        )

        write_tests = result.stage_by_ref_id("write_tests_core")
        assert write_tests is not None
        assert write_tests.status == WorkflowStatus.SUCCEEDED

        implement = result.stage_by_ref_id("implement_core")
        assert implement is not None
        assert implement.status == WorkflowStatus.SUCCEEDED

        assert CounterTask.counters.get("validate_errors") == 1
        assert CounterTask.counters.get("validate_backoff") == 2
        assert CounterTask.counters.get("validate_utils") == 1
        assert CounterTask.counters.get("validate_state") == 1
        assert CounterTask.counters.get("repair_errors") == 1
        assert CounterTask.counters.get("repair_utils") == 1
        assert CounterTask.counters.get("repair_state") == 1
        assert CounterTask.counters.get("write_tests_core") == 1
        assert CounterTask.counters.get("implement_core") == 1

    def test_backward_jump_within_single_branch_still_resets_chain(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """
        a -> b -> c (jumps to b once) -> d

        Standard backward jump in a linear chain should still reset c.
        This verifies we didn't break the normal case.
        """
        _reset_test_tasks()

        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"counter": CounterTask, "cond_jump": ConditionalJumpTask},
        )

        execution = Workflow.create(
            application="test",
            name="Linear Backward Jump",
            stages=[
                StageExecution(ref_id="a", tasks=_make_task("counter")),
                StageExecution(ref_id="b", requisite_stage_ref_ids={"a"}, tasks=_make_task("counter")),
                StageExecution(
                    ref_id="c",
                    requisite_stage_ref_ids={"b"},
                    context={"jump_target": "b", "_max_jumps": 2},
                    tasks=_make_task("cond_jump"),
                ),
                StageExecution(ref_id="d", requisite_stage_ref_ids={"c"}, tasks=_make_task("counter")),
            ],
        )

        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(execution.id)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert CounterTask.counters.get("a") == 1
        assert CounterTask.counters.get("b") == 2
        assert CounterTask.counters.get("d") == 1


class TestJoinResetOnRetryLoop:
    """Audit finding A2-10: a discriminator / N-of-M join inside a jump_to
    retry loop retained _join_fired=True after reset, so the join could
    never re-arm on the next iteration and the workflow wedged."""

    def test_reset_stage_for_retry_clears_join_tracking(self) -> None:
        from stabilize.handlers.jump_to_stage.reset import reset_stage_for_retry
        from stabilize.models.stage import JoinType, StageExecution

        stage = StageExecution(
            ref_id="join",
            name="Join",
            join_type=JoinType.DISCRIMINATOR,
            context={
                "_join_fired": True,
                "_completed_branches": ["a", "b"],
                "_activated_branches": ["a", "b"],
                "user_key": "kept",
            },
        )
        stage.status = WorkflowStatus.SUCCEEDED

        reset_stage_for_retry(stage)

        assert stage.status == WorkflowStatus.NOT_STARTED
        assert "_join_fired" not in stage.context, "join can never re-arm in the retry loop"
        assert "_completed_branches" not in stage.context
        assert "_activated_branches" not in stage.context
        assert stage.context["user_key"] == "kept"
