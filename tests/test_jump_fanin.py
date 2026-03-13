"""Tests for fan-in-aware jump traversal and backward jump with fan-in DAGs.

Covers the critical bug where backward jumps incorrectly reset fan-in stages
that have upstream dependencies from branches not involved in the jump.
"""

from stabilize import TaskResult
from stabilize.handlers.jump_to_stage.traversal import (
    get_downstream_stages,
    get_resettable_downstream_stages,
    get_skippable_downstream_stages,
    get_skipped_stages,
)
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.conftest import setup_stabilize


def _make_workflow(*stages: StageExecution) -> Workflow:
    return Workflow.create(application="test", name="test", stages=list(stages))


def _stage(ref_id: str, prereqs: set[str] | None = None) -> StageExecution:
    return StageExecution(ref_id=ref_id, requisite_stage_ref_ids=prereqs or set())


class TestGetResettableDownstreamStages:
    """Unit tests for get_resettable_downstream_stages (fan-in-aware)."""

    def test_linear_chain(self) -> None:
        """A -> B -> C: resetting A should include B and C."""
        wf = _make_workflow(
            _stage("a"),
            _stage("b", {"a"}),
            _stage("c", {"b"}),
        )
        result = get_resettable_downstream_stages(wf, "a")
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == {"b", "c"}

    def test_fan_in_with_external_upstream(self) -> None:
        """
        a1  a2  a3  a4
        |   |   |   |
        b1  b2  b3  b4
         \\  |  /   /
          fan_in --+

        Resetting b2 should NOT include fan_in (it depends on b1, b3, b4 too).
        """
        wf = _make_workflow(
            _stage("a1"),
            _stage("a2"),
            _stage("a3"),
            _stage("a4"),
            _stage("b1", {"a1"}),
            _stage("b2", {"a2"}),
            _stage("b3", {"a3"}),
            _stage("b4", {"a4"}),
            _stage("fan_in", {"b1", "b2", "b3", "b4"}),
        )
        result = get_resettable_downstream_stages(wf, "b2")
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == set()

    def test_fan_in_all_upstreams_in_scope(self) -> None:
        """
        a -> b1
        a -> b2
        b1, b2 -> fan_in

        Resetting a should include b1, b2, AND fan_in (all upstreams in scope).
        """
        wf = _make_workflow(
            _stage("a"),
            _stage("b1", {"a"}),
            _stage("b2", {"a"}),
            _stage("fan_in", {"b1", "b2"}),
        )
        result = get_resettable_downstream_stages(wf, "a")
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == {"b1", "b2", "fan_in"}

    def test_fan_in_partial_upstreams_excluded(self) -> None:
        """
        a -> b1 -------+
                        |
        x -> b2 -> fan_in -> downstream

        Resetting a: b1 is in scope, but fan_in needs {b1, b2} and b2 is NOT
        in scope (it depends on x). So fan_in and downstream are excluded.
        """
        wf = _make_workflow(
            _stage("a"),
            _stage("x"),
            _stage("b1", {"a"}),
            _stage("b2", {"x"}),
            _stage("fan_in", {"b1", "b2"}),
            _stage("downstream", {"fan_in"}),
        )
        result = get_resettable_downstream_stages(wf, "a")
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == {"b1"}

    def test_cascade_beyond_fan_in_excluded(self) -> None:
        """
        a -> b ----+
                   |
        x -> c -> fan_in -> d -> e

        Resetting a: b is in scope. fan_in needs {b, c}, c not in scope.
        fan_in excluded, so d and e are also excluded.
        """
        wf = _make_workflow(
            _stage("a"),
            _stage("x"),
            _stage("b", {"a"}),
            _stage("c", {"x"}),
            _stage("fan_in", {"b", "c"}),
            _stage("d", {"fan_in"}),
            _stage("e", {"d"}),
        )
        result = get_resettable_downstream_stages(wf, "a")
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == {"b"}

    def test_diamond_all_in_scope(self) -> None:
        """
        a -> b
        a -> c
        b, c -> d

        Resetting a includes b, c, and d (all paths from a).
        """
        wf = _make_workflow(
            _stage("a"),
            _stage("b", {"a"}),
            _stage("c", {"a"}),
            _stage("d", {"b", "c"}),
        )
        result = get_resettable_downstream_stages(wf, "a")
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == {"b", "c", "d"}

    def test_no_downstream(self) -> None:
        """Single stage with no dependents."""
        wf = _make_workflow(_stage("a"))
        result = get_resettable_downstream_stages(wf, "a")
        assert result == []

    def test_excludes_target_itself(self) -> None:
        """Target stage should not be in the result list."""
        wf = _make_workflow(
            _stage("a"),
            _stage("b", {"a"}),
        )
        result = get_resettable_downstream_stages(wf, "a")
        ref_ids = {s.ref_id for s in result}
        assert "a" not in ref_ids

    def test_real_world_dag_from_bug_report(self) -> None:
        """
        The exact DAG from the bug report:

        validate_errors  validate_backoff  validate_utils  validate_state
             |                |                 |               |
         repair_errors   repair_backoff    repair_utils    repair_state
                              ↑ jump target
              \\               |                /               /
               +---------write_tests_core---------+
                              |
                        implement_core
        """
        wf = _make_workflow(
            _stage("validate_errors"),
            _stage("validate_backoff"),
            _stage("validate_utils"),
            _stage("validate_state"),
            _stage("repair_errors", {"validate_errors"}),
            _stage("repair_backoff", {"validate_backoff"}),
            _stage("repair_utils", {"validate_utils"}),
            _stage("repair_state", {"validate_state"}),
            _stage("write_tests_core", {"repair_errors", "repair_backoff", "repair_utils", "repair_state"}),
            _stage("implement_core", {"write_tests_core"}),
        )
        result = get_resettable_downstream_stages(wf, "repair_backoff")
        ref_ids = {s.ref_id for s in result}
        assert "write_tests_core" not in ref_ids
        assert "implement_core" not in ref_ids
        assert ref_ids == set()

    def test_old_function_includes_fan_in_incorrectly(self) -> None:
        """Verify the old get_downstream_stages DOES include fan-in stages."""
        wf = _make_workflow(
            _stage("a"),
            _stage("x"),
            _stage("b1", {"a"}),
            _stage("b2", {"x"}),
            _stage("fan_in", {"b1", "b2"}),
        )
        old_result = get_downstream_stages(wf, "a")
        old_ref_ids = {s.ref_id for s in old_result}
        assert "fan_in" in old_ref_ids

        new_result = get_resettable_downstream_stages(wf, "a")
        new_ref_ids = {s.ref_id for s in new_result}
        assert "fan_in" not in new_ref_ids


class TestGetSkippableDownstreamStages:
    """Unit tests for get_skippable_downstream_stages (fan-in-aware)."""

    def test_linear_chain(self) -> None:
        wf = _make_workflow(
            _stage("a"),
            _stage("b", {"a"}),
            _stage("c", {"b"}),
        )
        result = get_skippable_downstream_stages(wf, "a")
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == {"b", "c"}

    def test_fan_in_excluded(self) -> None:
        """Source's exclusive downstream should not include fan-in with external upstreams."""
        wf = _make_workflow(
            _stage("a"),
            _stage("x"),
            _stage("b", {"a"}),
            _stage("c", {"x"}),
            _stage("fan_in", {"b", "c"}),
        )
        result = get_skippable_downstream_stages(wf, "a")
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == {"b"}


class TestGetSkippedStagesWithFanIn:
    """Test that get_skipped_stages respects fan-in boundaries."""

    def test_forward_jump_does_not_skip_fan_in_with_external_deps(self) -> None:
        """
        a -> b ----+
                   |
        x -> c -> fan_in -> target

        Forward jump from a to target should NOT skip fan_in (external dep c).
        """
        wf = _make_workflow(
            _stage("a"),
            _stage("x"),
            _stage("b", {"a"}),
            _stage("c", {"x"}),
            _stage("fan_in", {"b", "c"}),
            _stage("target", {"fan_in"}),
        )
        source = next(s for s in wf.stages if s.ref_id == "a")
        target = next(s for s in wf.stages if s.ref_id == "target")
        result = get_skipped_stages(wf, source, target)
        ref_ids = {s.ref_id for s in result}
        assert "fan_in" not in ref_ids
        assert ref_ids == {"b"}

    def test_forward_jump_skips_exclusive_downstream(self) -> None:
        """A -> B -> C, jump A to C: B is exclusively downstream of A, should be skipped."""
        wf = _make_workflow(
            _stage("a"),
            _stage("b", {"a"}),
            _stage("c", {"b"}),
        )
        source = next(s for s in wf.stages if s.ref_id == "a")
        target = next(s for s in wf.stages if s.ref_id == "c")
        result = get_skipped_stages(wf, source, target)
        ref_ids = {s.ref_id for s in result}
        assert ref_ids == {"b"}


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
