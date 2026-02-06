"""
Predicate-based readiness evaluation for stage execution.

Provides pure functions to evaluate whether a stage is ready to execute
based on its upstream dependencies. This logic is extracted from
StartStageHandler for better testability and reuse.

Usage:
    from stabilize.dag.readiness import evaluate_readiness, PredicatePhase

    result = evaluate_readiness(stage, upstream_stages)
    match result.phase:
        case PredicatePhase.READY:
            # Proceed with stage execution
            pass
        case PredicatePhase.NOT_READY:
            # Re-queue for later
            pass
        case PredicatePhase.SKIP:
            # Skip this stage
            pass
        case PredicatePhase.UNDEFINED:
            # Error during evaluation
            pass
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


class PredicatePhase(Enum):
    """Result of readiness evaluation.

    Determines what action to take for a stage based on upstream state.
    """

    READY = "READY"
    """All upstreams complete, proceed with execution."""

    NOT_READY = "NOT_READY"
    """Upstreams still running, re-queue for later."""

    SKIP = "SKIP"
    """Conditions not met (upstream halted), skip this stage."""

    UNDEFINED = "UNDEFINED"
    """Error during evaluation or inconsistent state."""


@dataclass(frozen=True)
class ReadinessResult:
    """Result of readiness evaluation.

    Contains the phase decision and supporting information for
    logging, debugging, and downstream handling.

    Attributes:
        phase: The readiness decision
        reason: Human-readable explanation of the decision
        failed_upstream_ids: IDs of upstream stages that caused SKIP
        active_upstream_ids: IDs of upstream stages still running (NOT_READY)
    """

    phase: PredicatePhase
    reason: str = ""
    failed_upstream_ids: list[str] = field(default_factory=list)
    active_upstream_ids: list[str] = field(default_factory=list)


def evaluate_readiness(
    stage: StageExecution,
    upstream_stages: list[StageExecution],
    jump_bypass: bool = False,
) -> ReadinessResult:
    """Pure function: check if stage is ready to execute.

    Logic extracted from StartStageHandler.handle():
    1. If jump_bypass, return READY (dynamic routing override)
    2. If any upstream halted (TERMINAL/CANCELED/STOPPED), return SKIP
    3. If any upstream not complete, return NOT_READY
    4. If all upstream complete/skipped, return READY

    This function is pure (no side effects) and can be easily tested
    in isolation without mocking handlers or repositories.

    Args:
        stage: The stage being evaluated
        upstream_stages: List of stages this stage depends on
        jump_bypass: If True, skip prerequisite checks (for dynamic routing)

    Returns:
        ReadinessResult with phase and supporting information.

    Example:
        upstream = repository.get_upstream_stages(execution_id, stage.ref_id)
        result = evaluate_readiness(stage, upstream)

        if result.phase == PredicatePhase.SKIP:
            logger.warning(
                "Skipping %s due to failed upstreams: %s",
                stage.name,
                result.failed_upstream_ids
            )
    """
    from stabilize.models.status import ACTIVE_STATUSES, CONTINUABLE_STATUSES, HALT_STATUSES

    # Early exit: jump bypass skips all prerequisite checks
    if jump_bypass:
        return ReadinessResult(
            phase=PredicatePhase.READY,
            reason="Jump bypass enabled, skipping prerequisite checks",
        )

    # No upstream dependencies = ready
    if not upstream_stages:
        return ReadinessResult(
            phase=PredicatePhase.READY,
            reason="No upstream dependencies",
        )

    # Check for halted upstreams (TERMINAL, STOPPED, CANCELED)
    failed_ids: list[str] = []
    for upstream in upstream_stages:
        if upstream is None:
            continue
        if upstream.status in HALT_STATUSES:
            failed_ids.append(upstream.id)

    if failed_ids:
        return ReadinessResult(
            phase=PredicatePhase.SKIP,
            reason=f"Upstream stages halted: {', '.join(failed_ids)}",
            failed_upstream_ids=failed_ids,
        )

    # Check if all upstreams are complete (SUCCEEDED, FAILED_CONTINUE, SKIPPED)
    active_ids: list[str] = []
    not_complete_ids: list[str] = []

    for upstream in upstream_stages:
        if upstream is None:
            continue
        if upstream.status not in CONTINUABLE_STATUSES:
            not_complete_ids.append(upstream.id)
            if upstream.status in ACTIVE_STATUSES:
                active_ids.append(upstream.id)

    if not not_complete_ids:
        return ReadinessResult(
            phase=PredicatePhase.READY,
            reason="All upstream stages complete",
        )

    # Upstreams not complete
    if active_ids:
        return ReadinessResult(
            phase=PredicatePhase.NOT_READY,
            reason=f"Upstream stages still active: {', '.join(active_ids)}",
            active_upstream_ids=active_ids,
        )

    # Some upstreams not complete but not active either (stuck?)
    return ReadinessResult(
        phase=PredicatePhase.NOT_READY,
        reason=f"Upstream stages not complete: {', '.join(not_complete_ids)}",
        active_upstream_ids=not_complete_ids,
    )


def check_recursive_failures(
    stage: StageExecution,
    upstream_stages: list[StageExecution],
) -> list[str]:
    """Check for halted upstreams recursively through NOT_STARTED stages.

    When an upstream stage is NOT_STARTED, we need to check if any of
    its upstreams have failed, as the failure may not have propagated yet.

    Args:
        stage: The stage being evaluated
        upstream_stages: List of direct upstream stages

    Returns:
        List of failed upstream stage IDs found recursively.
    """
    from stabilize.models.status import HALT_STATUSES, WorkflowStatus

    failed_ids: list[str] = []

    for upstream in upstream_stages:
        if upstream is None:
            continue

        if upstream.status in HALT_STATUSES:
            failed_ids.append(upstream.id)
        elif upstream.status == WorkflowStatus.NOT_STARTED:
            # Recursively check this upstream's dependencies
            if upstream.any_upstream_stages_failed():
                failed_ids.append(upstream.id)

    return failed_ids
