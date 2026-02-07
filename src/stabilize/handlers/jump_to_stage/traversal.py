"""DAG traversal utilities for jump operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow


def get_downstream_stages(
    execution: Workflow,
    target_ref_id: str,
) -> list[StageExecution]:
    """Get all stages that depend on the target stage (directly or transitively).

    This is used to reset downstream stages when jumping back to an earlier
    stage in a retry loop, ensuring verification/aggregation stages re-run.

    Args:
        execution: The workflow execution containing all stages
        target_ref_id: The ref_id of the target stage

    Returns:
        List of stages that depend on the target (directly or transitively)
    """
    downstream: list[StageExecution] = []
    visited: set[str] = set()

    def find_dependents(ref_id: str) -> None:
        for stage in execution.stages:
            if stage.ref_id in visited:
                continue
            prereqs = stage.requisite_stage_ref_ids
            if ref_id in prereqs:
                visited.add(stage.ref_id)
                downstream.append(stage)
                find_dependents(stage.ref_id)

    find_dependents(target_ref_id)
    return downstream


def get_skipped_stages(
    execution: Workflow,
    source_stage: StageExecution,
    target_stage: StageExecution,
) -> list[StageExecution]:
    """Get stages that should be skipped when jumping forward.

    These are stages that depend on the source (directly or transitively)
    but are NOT the target or in the target's downstream chain.

    Args:
        execution: The workflow execution containing all stages
        source_stage: The stage jumping from
        target_stage: The stage jumping to

    Returns:
        List of stages to mark as SKIPPED
    """
    # Get all stages downstream of source (depend on source)
    source_downstream = set(s.ref_id for s in get_downstream_stages(execution, source_stage.ref_id))

    # Get target and all stages downstream of target
    target_chain = {target_stage.ref_id}
    for s in get_downstream_stages(execution, target_stage.ref_id):
        target_chain.add(s.ref_id)

    # Skipped = downstream of source BUT NOT in target chain
    skipped_ref_ids = source_downstream - target_chain

    return [s for s in execution.stages if s.ref_id in skipped_ref_ids]
