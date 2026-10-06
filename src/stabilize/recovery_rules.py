"""Stage-level rules recovery uses to decide what to re-queue."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow

logger = logging.getLogger("stabilize.recovery")


def has_started(stage: StageExecution) -> bool:
    """Check if a stage has actually started execution.

    A stage may be in NOT_STARTED status but have a start_time,
    indicating it was being processed when the crash occurred.

    Args:
        stage: The stage to check

    Returns:
        True if stage has evidence of starting
    """
    if stage.start_time is not None:
        return True

    # Check if any tasks have started
    for task in stage.tasks:
        if task.start_time is not None:
            return True

    return False

def can_start(stage: StageExecution, workflow: Workflow) -> bool:
    """Check if a stage's dependencies are met and it can start.

    A stage can start if:
    - It has no dependencies (initial stage), OR
    - All upstream stages are in CONTINUABLE_STATUSES (SUCCEEDED, FAILED_CONTINUE, SKIPPED, REDIRECT)
    - Join-type specific conditions are satisfied (DISCRIMINATOR not already fired, N_OF_M threshold met)

    Args:
        stage: The stage to check
        workflow: The full workflow containing all stages

    Returns:
        True if stage dependencies are met
    """
    from stabilize.models.stage import JoinType
    from stabilize.models.status import CONTINUABLE_STATUSES

    # No dependencies - can always start
    if not stage.requisite_stage_ref_ids:
        return True

    # DISCRIMINATOR / N_OF_M that already fired should not be re-queued
    if stage.join_type in (JoinType.DISCRIMINATOR, JoinType.N_OF_M):
        if stage.context.get("_join_fired", False):
            return False

    # Check all upstream stages
    upstream_stages = []
    for ref_id in stage.requisite_stage_ref_ids:
        upstream = next((s for s in workflow.stages if s.ref_id == ref_id), None)
        if upstream is None:
            logger.error(
                "Stage %s depends on unknown stage %s — possible workflow definition error",
                stage.ref_id,
                ref_id,
            )
            return False
        upstream_stages.append(upstream)

    # N_OF_M: check threshold
    if stage.join_type == JoinType.N_OF_M:
        threshold = stage.join_threshold
        if threshold > len(upstream_stages):
            logger.error(
                "Stage %s has join_threshold=%d but only %d upstreams — unreachable",
                stage.ref_id,
                threshold,
                len(upstream_stages),
            )
            return False
        completed = sum(1 for u in upstream_stages if u.status in CONTINUABLE_STATUSES)
        return completed >= threshold

    # Default (AND join): all upstreams must be complete
    for upstream in upstream_stages:
        if upstream.status not in CONTINUABLE_STATUSES:
            return False

    return True
