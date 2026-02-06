"""Tests for predicate-based readiness evaluation."""

import pytest

from stabilize.dag.readiness import (
    PredicatePhase,
    ReadinessResult,
    evaluate_readiness,
)
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus


def make_stage(
    status: WorkflowStatus = WorkflowStatus.NOT_STARTED,
    ref_id: str = "stage-1",
) -> StageExecution:
    """Create a stage with given status for testing."""
    stage = StageExecution(
        name="test",
        type="test",
        ref_id=ref_id,
        status=status,
    )
    return stage


class TestPredicatePhase:
    """Tests for PredicatePhase enum."""

    def test_all_phases_have_values(self) -> None:
        """All phases have string values."""
        for phase in PredicatePhase:
            assert isinstance(phase.value, str)

    def test_expected_phases_exist(self) -> None:
        """Expected phases are defined."""
        assert PredicatePhase.READY
        assert PredicatePhase.NOT_READY
        assert PredicatePhase.SKIP
        assert PredicatePhase.UNDEFINED


class TestEvaluateReadiness:
    """Tests for evaluate_readiness() function."""

    def test_no_upstreams_is_ready(self) -> None:
        """Stage with no upstreams is READY."""
        stage = make_stage()
        result = evaluate_readiness(stage, [])

        assert result.phase == PredicatePhase.READY
        assert "No upstream" in result.reason

    def test_jump_bypass_is_always_ready(self) -> None:
        """Jump bypass skips all checks and returns READY."""
        stage = make_stage()
        # Even with failed upstreams, jump_bypass returns READY
        failed_upstream = make_stage(WorkflowStatus.TERMINAL, "upstream-1")

        result = evaluate_readiness(stage, [failed_upstream], jump_bypass=True)

        assert result.phase == PredicatePhase.READY
        assert "bypass" in result.reason.lower()

    def test_all_upstreams_complete_is_ready(self) -> None:
        """Stage with all complete upstreams is READY."""
        stage = make_stage()
        upstream1 = make_stage(WorkflowStatus.SUCCEEDED, "upstream-1")
        upstream2 = make_stage(WorkflowStatus.SKIPPED, "upstream-2")

        result = evaluate_readiness(stage, [upstream1, upstream2])

        assert result.phase == PredicatePhase.READY
        assert "complete" in result.reason.lower()

    def test_terminal_upstream_causes_skip(self) -> None:
        """Terminal upstream causes SKIP."""
        stage = make_stage()
        upstream = make_stage(WorkflowStatus.TERMINAL, "upstream-1")

        result = evaluate_readiness(stage, [upstream])

        assert result.phase == PredicatePhase.SKIP
        assert upstream.id in result.failed_upstream_ids

    def test_canceled_upstream_causes_skip(self) -> None:
        """Canceled upstream causes SKIP."""
        stage = make_stage()
        upstream = make_stage(WorkflowStatus.CANCELED, "upstream-1")

        result = evaluate_readiness(stage, [upstream])

        assert result.phase == PredicatePhase.SKIP
        assert upstream.id in result.failed_upstream_ids

    def test_stopped_upstream_causes_skip(self) -> None:
        """Stopped upstream causes SKIP."""
        stage = make_stage()
        upstream = make_stage(WorkflowStatus.STOPPED, "upstream-1")

        result = evaluate_readiness(stage, [upstream])

        assert result.phase == PredicatePhase.SKIP
        assert upstream.id in result.failed_upstream_ids

    def test_running_upstream_is_not_ready(self) -> None:
        """Running upstream causes NOT_READY."""
        stage = make_stage()
        upstream = make_stage(WorkflowStatus.RUNNING, "upstream-1")

        result = evaluate_readiness(stage, [upstream])

        assert result.phase == PredicatePhase.NOT_READY
        assert upstream.id in result.active_upstream_ids

    def test_not_started_upstream_is_not_ready(self) -> None:
        """NOT_STARTED upstream causes NOT_READY."""
        stage = make_stage()
        upstream = make_stage(WorkflowStatus.NOT_STARTED, "upstream-1")

        result = evaluate_readiness(stage, [upstream])

        assert result.phase == PredicatePhase.NOT_READY

    def test_mixed_upstreams_halted_takes_precedence(self) -> None:
        """Halted upstream takes precedence over incomplete."""
        stage = make_stage()
        running = make_stage(WorkflowStatus.RUNNING, "running")
        terminal = make_stage(WorkflowStatus.TERMINAL, "terminal")

        result = evaluate_readiness(stage, [running, terminal])

        assert result.phase == PredicatePhase.SKIP
        assert terminal.id in result.failed_upstream_ids

    def test_none_upstreams_ignored(self) -> None:
        """None values in upstream list are ignored."""
        stage = make_stage()
        upstream = make_stage(WorkflowStatus.SUCCEEDED, "upstream-1")

        result = evaluate_readiness(stage, [None, upstream, None])  # type: ignore

        assert result.phase == PredicatePhase.READY


class TestReadinessResult:
    """Tests for ReadinessResult dataclass."""

    def test_frozen_dataclass(self) -> None:
        """ReadinessResult is immutable."""
        result = ReadinessResult(phase=PredicatePhase.READY, reason="test")

        with pytest.raises(Exception):  # FrozenInstanceError
            result.phase = PredicatePhase.SKIP  # type: ignore

    def test_default_values(self) -> None:
        """Default values are correct."""
        result = ReadinessResult(phase=PredicatePhase.READY)

        assert result.reason == ""
        assert result.failed_upstream_ids == []
        assert result.active_upstream_ids == []
