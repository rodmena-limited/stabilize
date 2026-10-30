from stabilize.handlers.jump_to_stage.traversal import (
    get_downstream_stages,
    get_resettable_downstream_stages,
    get_skippable_downstream_stages,
    get_skipped_stages,
)
from stabilize.models.stage import StageExecution
from stabilize.models.workflow import Workflow


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
