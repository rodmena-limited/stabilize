from __future__ import annotations

from typing import Any

from stabilize import (
    JoinType,
    SplitType,
    StageExecution,
    TaskResult,
)
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import setup_stabilize
from tests.control_flow_support import OutputTask

# =============================================================================
# Section 12: TaskResult.suspend() Tests
# =============================================================================


class TestTaskResultSuspend:
    """Test TaskResult.suspend() factory method."""

    def test_suspend_creates_suspended_result(self) -> None:
        result = TaskResult.suspend()
        assert result.status == WorkflowStatus.SUSPENDED
        assert result.context == {}

    def test_suspend_with_context(self) -> None:
        result = TaskResult.suspend(context={"waiting_for": "approval"})
        assert result.status == WorkflowStatus.SUSPENDED
        assert result.context == {"waiting_for": "approval"}


# =============================================================================
# Section 13: Readiness Evaluation Unit Tests
# =============================================================================


class TestReadinessEvaluation:
    """Unit tests for readiness evaluation with different join types."""

    def _make_stage(
        self,
        ref_id: str,
        status: WorkflowStatus = WorkflowStatus.NOT_STARTED,
        join_type: JoinType = JoinType.AND,
        join_threshold: int = 0,
        context: dict[str, Any] | None = None,
    ) -> StageExecution:
        """Create a minimal stage for readiness testing."""
        stage = StageExecution(
            ref_id=ref_id,
            name=f"Stage {ref_id}",
            status=status,
            join_type=join_type,
            join_threshold=join_threshold,
            context=context or {},
        )
        return stage

    def test_and_join_waits_for_all(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join")
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.NOT_READY

    def test_and_join_ready_when_all_complete(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join")
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.SUCCEEDED)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.READY

    def test_and_join_skip_on_halted(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join")
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.TERMINAL)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.SKIP

    def test_or_join_with_activated_branches(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage(
            "join",
            join_type=JoinType.OR,
            context={"_activated_branches": ["a"]},
        )
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.NOT_STARTED)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.READY

    def test_or_join_falls_back_to_and_without_activation_info(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.OR)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.NOT_READY

    def test_discriminator_fires_on_first(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.DISCRIMINATOR)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.READY

    def test_discriminator_ignores_after_fired(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage(
            "join",
            join_type=JoinType.DISCRIMINATOR,
            context={"_join_fired": True},
        )
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.SUCCEEDED)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.NOT_READY

    def test_n_of_m_fires_at_threshold(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.N_OF_M, join_threshold=2)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.SUCCEEDED)
        upstream_c = self._make_stage("c", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b, upstream_c])
        assert result.phase == PredicatePhase.READY

    def test_n_of_m_not_ready_below_threshold(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.N_OF_M, join_threshold=2)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)
        upstream_c = self._make_stage("c", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b, upstream_c])
        assert result.phase == PredicatePhase.NOT_READY

    def test_n_of_m_skip_when_threshold_unachievable(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.N_OF_M, join_threshold=3)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.TERMINAL)
        upstream_c = self._make_stage("c", WorkflowStatus.TERMINAL)

        result = evaluate_readiness(stage, [upstream_a, upstream_b, upstream_c])
        assert result.phase == PredicatePhase.SKIP

    def test_multi_merge_fires_on_any_completion(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.MULTI_MERGE)
        upstream_a = self._make_stage("a", WorkflowStatus.SUCCEEDED)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.READY

    def test_multi_merge_not_ready_when_none_complete(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join", join_type=JoinType.MULTI_MERGE)
        upstream_a = self._make_stage("a", WorkflowStatus.RUNNING)
        upstream_b = self._make_stage("b", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream_a, upstream_b])
        assert result.phase == PredicatePhase.NOT_READY

    def test_jump_bypass_always_ready(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("join")
        upstream = self._make_stage("a", WorkflowStatus.RUNNING)

        result = evaluate_readiness(stage, [upstream], jump_bypass=True)
        assert result.phase == PredicatePhase.READY

    def test_no_upstreams_always_ready(self) -> None:
        from stabilize.dag.readiness import PredicatePhase, evaluate_readiness

        stage = self._make_stage("initial")
        result = evaluate_readiness(stage, [])
        assert result.phase == PredicatePhase.READY


# =============================================================================
# Section 14: Model Field Tests
# =============================================================================


class TestModelFields:
    """Test that new fields on StageExecution serialize/persist correctly."""

    def test_join_type_default(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.join_type == JoinType.AND

    def test_split_type_default(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.split_type == SplitType.AND

    def test_new_fields_default_to_none(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.mi_config is None
        assert stage.deferred_choice_group is None
        assert stage.milestone_ref_id is None
        assert stage.milestone_status is None
        assert stage.mutex_key is None
        assert stage.cancel_region is None

    def test_join_threshold_default(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.join_threshold == 0

    def test_split_conditions_default(self) -> None:
        stage = StageExecution(ref_id="test", name="Test")
        assert stage.split_conditions == {}

    def test_stage_persistence_roundtrip(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        """New fields should survive store/retrieve roundtrip."""
        processor, runner, _ = setup_stabilize(
            repository,
            queue,
            extra_tasks={"output": OutputTask},
        )

        wf = Workflow.create(
            application="wcp-test",
            name="roundtrip-test",
            stages=[
                StageExecution(
                    ref_id="root",
                    name="Root",
                    join_type=JoinType.DISCRIMINATOR,
                    join_threshold=3,
                    split_type=SplitType.OR,
                    split_conditions={"a": "x > 5", "b": "y < 10"},
                    deferred_choice_group="group_1",
                    milestone_ref_id="checkpoint",
                    milestone_status="RUNNING",
                    mutex_key="shared_lock",
                    cancel_region="region_A",
                    context={"outputs_to_produce": {"from_root": True}},
                    tasks=[
                        TaskExecution.create(
                            "Root Task",
                            "output",
                            stage_start=True,
                            stage_end=True,
                        )
                    ],
                ),
            ],
        )

        repository.store(wf)
        result = repository.retrieve(wf.id)

        root = result.stage_by_ref_id("root")
        assert root.join_type == JoinType.DISCRIMINATOR
        assert root.join_threshold == 3
        assert root.split_type == SplitType.OR
        assert root.split_conditions == {"a": "x > 5", "b": "y < 10"}
        assert root.deferred_choice_group == "group_1"
        assert root.milestone_ref_id == "checkpoint"
        assert root.milestone_status == "RUNNING"
        assert root.mutex_key == "shared_lock"
        assert root.cancel_region == "region_A"
