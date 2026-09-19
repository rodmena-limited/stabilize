"""Ticket #4: a structurally impossible stage graph must be refused at submit
time, not discovered at runtime as "Exceeded max retries waiting for upstream
stages" — which is indistinguishable from legitimate contention."""

from __future__ import annotations

import pytest

from stabilize.dag.topological import (
    CircularDependencyError,
    InvalidStageGraphError,
    validate_stage_graph,
)
from stabilize.models import StageExecution, Workflow
from stabilize.models.stage.enums import JoinType


def _stage(
    ref: str,
    requisites: set[str] | None = None,
    join_type: JoinType | None = None,
    threshold: int = 0,
) -> StageExecution:
    stage = StageExecution(ref_id=ref, name=f"stage-{ref}", type="test")
    stage.requisite_stage_ref_ids = set(requisites or ())
    if join_type is not None:
        stage.join_type = join_type
        stage.join_threshold = threshold
    return stage


class TestStructuralDefectsAreRefusedAtSubmit:
    def test_duplicate_ref_id(self) -> None:
        with pytest.raises(InvalidStageGraphError, match="duplicate_ref"):
            Workflow.create(application="a", name="n", stages=[_stage("1"), _stage("1")])

    def test_self_edge(self) -> None:
        with pytest.raises(InvalidStageGraphError, match="self_edge"):
            Workflow.create(application="a", name="n", stages=[_stage("1", {"1"})])

    def test_unknown_requisite_names_the_missing_ref(self) -> None:
        with pytest.raises(InvalidStageGraphError, match="nope"):
            Workflow.create(application="a", name="n", stages=[_stage("1", {"nope"})])

    def test_two_cycle_names_its_members(self) -> None:
        with pytest.raises(CircularDependencyError) as excinfo:
            Workflow.create(
                application="a", name="n", stages=[_stage("1", {"2"}), _stage("2", {"1"})]
            )
        assert "1" in str(excinfo.value) and "2" in str(excinfo.value)

    def test_three_cycle_is_caught(self) -> None:
        with pytest.raises(CircularDependencyError):
            Workflow.create(
                application="a",
                name="n",
                stages=[_stage("1", {"3"}), _stage("2", {"1"}), _stage("3", {"2"})],
            )


class TestUnreachableJoin:
    """Condition-aware reachability, for the decidable case."""

    def test_threshold_above_upstream_count_is_refused(self) -> None:
        with pytest.raises(InvalidStageGraphError, match="unreachable_join"):
            Workflow.create(
                application="a",
                name="n",
                stages=[
                    _stage("1"),
                    _stage("2"),
                    _stage("3", {"1", "2"}, JoinType.N_OF_M, 3),
                ],
            )

    def test_error_names_the_stage_and_both_numbers(self) -> None:
        with pytest.raises(InvalidStageGraphError) as excinfo:
            validate_stage_graph(
                [_stage("1"), _stage("2"), _stage("gate", {"1", "2"}, JoinType.N_OF_M, 5)]
            )
        message = str(excinfo.value)
        assert "gate" in message and "5" in message and "2" in message

    @pytest.mark.parametrize("threshold", [1, 2])
    def test_satisfiable_thresholds_are_accepted(self, threshold: int) -> None:
        """Without this the check could pass by refusing every N_OF_M join."""
        workflow = Workflow.create(
            application="a",
            name="n",
            stages=[
                _stage("1"),
                _stage("2"),
                _stage("3", {"1", "2"}, JoinType.N_OF_M, threshold),
            ],
        )
        assert workflow is not None

    def test_other_join_types_are_untouched(self) -> None:
        workflow = Workflow.create(
            application="a",
            name="n",
            stages=[_stage("1"), _stage("2", {"1"}, JoinType.AND, 99)],
        )
        assert workflow is not None


class TestValidGraphsStillPass:
    def test_linear_chain(self) -> None:
        assert Workflow.create(
            application="a", name="n", stages=[_stage("1"), _stage("2", {"1"})]
        )

    def test_diamond(self) -> None:
        assert Workflow.create(
            application="a",
            name="n",
            stages=[
                _stage("1"),
                _stage("2", {"1"}),
                _stage("3", {"1"}),
                _stage("4", {"2", "3"}),
            ],
        )

    def test_single_stage(self) -> None:
        assert Workflow.create(application="a", name="n", stages=[_stage("1")])
