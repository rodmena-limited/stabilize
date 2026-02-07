"""
Predicate-based readiness evaluation for stage execution.

Provides pure functions to evaluate whether a stage is ready to execute
based on its upstream dependencies. This logic is extracted from
StartStageHandler for better testability and reuse.

Supports multiple join types for advanced control-flow patterns:
- AND join (WCP-3): Wait for ALL upstreams
- OR join (WCP-7): Wait only for activated branches
- MULTI_MERGE (WCP-8): Fire once per upstream completion
- DISCRIMINATOR (WCP-9): Fire on first upstream, ignore rest
- N_OF_M (WCP-30): Fire when N upstreams complete

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

    Dispatches to the appropriate join-type evaluator based on stage.join_type.

    Args:
        stage: The stage being evaluated
        upstream_stages: List of stages this stage depends on
        jump_bypass: If True, skip prerequisite checks (for dynamic routing)

    Returns:
        ReadinessResult with phase and supporting information.
    """
    from stabilize.models.stage import JoinType

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

    # Dispatch based on join type
    join_type = stage.join_type

    if join_type == JoinType.OR:
        return _evaluate_or_join(stage, upstream_stages)
    elif join_type == JoinType.MULTI_MERGE:
        return _evaluate_multi_merge(stage, upstream_stages)
    elif join_type == JoinType.DISCRIMINATOR:
        return _evaluate_discriminator(stage, upstream_stages)
    elif join_type == JoinType.N_OF_M:
        return _evaluate_n_of_m(stage, upstream_stages)
    else:
        # Default: AND join
        return _evaluate_and_join(stage, upstream_stages)


def _evaluate_and_join(
    stage: StageExecution,
    upstream_stages: list[StageExecution],
) -> ReadinessResult:
    """AND-join (WCP-3): Wait for ALL upstreams to complete.

    Logic:
    1. If any upstream halted (TERMINAL/CANCELED/STOPPED), return SKIP
    2. If all upstreams in CONTINUABLE_STATUSES, return READY
    3. Otherwise, return NOT_READY
    """
    from stabilize.models.status import (
        ACTIVE_STATUSES,
        CONTINUABLE_STATUSES,
        HALT_STATUSES,
    )

    # Check for halted upstreams
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

    # Check if all upstreams are complete
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


def _evaluate_or_join(
    stage: StageExecution,
    upstream_stages: list[StageExecution],
) -> ReadinessResult:
    """OR-join (WCP-7): Wait only for branches activated by the paired OR-split.

    The paired OR-split records activated branch ref_ids in the join stage's
    context as '_activated_branches'. Only those upstreams are checked.
    If '_activated_branches' is not set, falls back to AND-join behavior.
    """
    from stabilize.models.status import (
        ACTIVE_STATUSES,
        CONTINUABLE_STATUSES,
        HALT_STATUSES,
    )

    activated_branches: list[str] | None = stage.context.get("_activated_branches")

    if activated_branches is None:
        # No activation info - fall back to AND-join
        return _evaluate_and_join(stage, upstream_stages)

    activated_set = set(activated_branches)

    # Filter upstream stages to only activated ones
    relevant_upstreams = [
        u for u in upstream_stages if u is not None and u.ref_id in activated_set
    ]

    if not relevant_upstreams:
        return ReadinessResult(
            phase=PredicatePhase.READY,
            reason="No activated branches to wait for",
        )

    # Check for halted activated upstreams
    failed_ids: list[str] = []
    for upstream in relevant_upstreams:
        if upstream.status in HALT_STATUSES:
            failed_ids.append(upstream.id)

    if failed_ids:
        return ReadinessResult(
            phase=PredicatePhase.SKIP,
            reason=f"Activated upstream stages halted: {', '.join(failed_ids)}",
            failed_upstream_ids=failed_ids,
        )

    # Check if all activated upstreams are complete
    not_complete_ids: list[str] = []
    active_ids: list[str] = []

    for upstream in relevant_upstreams:
        if upstream.status not in CONTINUABLE_STATUSES:
            not_complete_ids.append(upstream.id)
            if upstream.status in ACTIVE_STATUSES:
                active_ids.append(upstream.id)

    if not not_complete_ids:
        return ReadinessResult(
            phase=PredicatePhase.READY,
            reason="All activated upstream stages complete",
        )

    if active_ids:
        return ReadinessResult(
            phase=PredicatePhase.NOT_READY,
            reason=f"Activated upstream stages still active: {', '.join(active_ids)}",
            active_upstream_ids=active_ids,
        )

    return ReadinessResult(
        phase=PredicatePhase.NOT_READY,
        reason=f"Activated upstream stages not complete: {', '.join(not_complete_ids)}",
        active_upstream_ids=not_complete_ids,
    )


def _evaluate_multi_merge(
    stage: StageExecution,
    upstream_stages: list[StageExecution],
) -> ReadinessResult:
    """Multi-merge (WCP-8): Fire once per upstream completion.

    Returns READY immediately - each upstream completion triggers a separate
    StartStage message. The stage is re-executable (reset after each firing).
    The caller is responsible for tracking which upstream triggered this evaluation.
    """
    from stabilize.models.status import CONTINUABLE_STATUSES, HALT_STATUSES

    # Check if at least one upstream has completed
    for upstream in upstream_stages:
        if upstream is None:
            continue
        if upstream.status in CONTINUABLE_STATUSES:
            return ReadinessResult(
                phase=PredicatePhase.READY,
                reason=f"Multi-merge: upstream {upstream.id} completed",
            )

    # Check if all upstreams halted
    all_halted = all(u.status in HALT_STATUSES for u in upstream_stages if u is not None)
    if all_halted:
        return ReadinessResult(
            phase=PredicatePhase.SKIP,
            reason="All upstream stages halted",
            failed_upstream_ids=[u.id for u in upstream_stages if u is not None],
        )

    return ReadinessResult(
        phase=PredicatePhase.NOT_READY,
        reason="No upstream stages have completed yet",
        active_upstream_ids=[u.id for u in upstream_stages if u is not None],
    )


def _evaluate_discriminator(
    stage: StageExecution,
    upstream_stages: list[StageExecution],
) -> ReadinessResult:
    """Discriminator (WCP-9): Fire on first upstream completion, ignore rest.

    Uses '_join_fired' context flag to track if the discriminator has already
    fired. Returns SKIP for subsequent completions until all upstreams are done
    (reset).

    Also supports blocking discriminator (WCP-28) via '_join_blocking' context,
    and cancelling discriminator (WCP-29) via stage context flag.

    CONCURRENCY NOTE: This is a pure function that reads '_join_fired' from context.
    The CALLER is responsible for atomically setting '_join_fired=True' using
    store_stage(stage, expected_phase=...) AFTER this function returns READY and
    BEFORE proceeding with execution. Failure to do so will cause race conditions
    where multiple upstreams can fire the discriminator simultaneously.
    """
    from stabilize.models.status import CONTINUABLE_STATUSES, HALT_STATUSES

    join_fired = stage.context.get("_join_fired", False)

    if join_fired:
        # Already fired - check if all upstreams are done (reset condition)
        all_done = all(u.status.is_complete for u in upstream_stages if u is not None)
        if all_done:
            # Reset: clear the flag so discriminator can fire again
            # Caller should handle this
            return ReadinessResult(
                phase=PredicatePhase.NOT_READY,
                reason="Discriminator already fired, waiting for reset (all upstreams done)",
            )
        return ReadinessResult(
            phase=PredicatePhase.NOT_READY,
            reason="Discriminator already fired, ignoring subsequent completions",
        )

    # Not yet fired - check if any upstream has completed
    for upstream in upstream_stages:
        if upstream is None:
            continue
        if upstream.status in CONTINUABLE_STATUSES:
            return ReadinessResult(
                phase=PredicatePhase.READY,
                reason=f"Discriminator: first upstream {upstream.id} completed",
            )

    # Check if all halted
    all_halted = all(u.status in HALT_STATUSES for u in upstream_stages if u is not None)
    if all_halted:
        return ReadinessResult(
            phase=PredicatePhase.SKIP,
            reason="All upstream stages halted before discriminator fired",
            failed_upstream_ids=[u.id for u in upstream_stages if u is not None],
        )

    return ReadinessResult(
        phase=PredicatePhase.NOT_READY,
        reason="Discriminator waiting for first upstream to complete",
        active_upstream_ids=[u.id for u in upstream_stages if u is not None],
    )


def _evaluate_n_of_m(
    stage: StageExecution,
    upstream_stages: list[StageExecution],
) -> ReadinessResult:
    """N-of-M join (WCP-30): Fire when N of M upstreams complete.

    Uses stage.join_threshold as N. Tracks completed branches in
    '_completed_branches' context key.

    CONCURRENCY NOTE: This is a pure function that reads '_join_fired' from context.
    The CALLER is responsible for atomically setting '_join_fired=True' using
    store_stage(stage, expected_phase=...) AFTER this function returns READY and
    BEFORE proceeding with execution. Failure to do so will cause race conditions
    where multiple upstreams can fire the N-of-M join simultaneously.
    """
    from stabilize.models.status import CONTINUABLE_STATUSES, HALT_STATUSES

    threshold = stage.join_threshold
    if threshold <= 0:
        # Invalid threshold, fall back to AND-join
        return _evaluate_and_join(stage, upstream_stages)

    join_fired = stage.context.get("_join_fired", False)
    if join_fired:
        return ReadinessResult(
            phase=PredicatePhase.NOT_READY,
            reason="N-of-M join already fired",
        )

    # Count completed upstreams
    completed_ids: list[str] = []
    failed_ids: list[str] = []
    active_ids: list[str] = []

    for upstream in upstream_stages:
        if upstream is None:
            continue
        if upstream.status in CONTINUABLE_STATUSES:
            completed_ids.append(upstream.id)
        elif upstream.status in HALT_STATUSES:
            failed_ids.append(upstream.id)
        else:
            active_ids.append(upstream.id)

    if len(completed_ids) >= threshold:
        return ReadinessResult(
            phase=PredicatePhase.READY,
            reason=f"N-of-M join: {len(completed_ids)}/{threshold} upstreams complete",
        )

    # Check if threshold is still achievable
    remaining_possible = len(completed_ids) + len(active_ids)
    if remaining_possible < threshold:
        return ReadinessResult(
            phase=PredicatePhase.SKIP,
            reason=f"N-of-M join: only {remaining_possible} possible completions, need {threshold}",
            failed_upstream_ids=failed_ids,
        )

    if active_ids:
        return ReadinessResult(
            phase=PredicatePhase.NOT_READY,
            reason=f"N-of-M join: {len(completed_ids)}/{threshold} complete, waiting",
            active_upstream_ids=active_ids,
        )

    return ReadinessResult(
        phase=PredicatePhase.NOT_READY,
        reason=f"N-of-M join: {len(completed_ids)}/{threshold} complete, {len(failed_ids)} failed",
        active_upstream_ids=[],
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
