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

    WARNING: This performs a naive transitive closure that does NOT respect
    fan-in boundaries. A fan-in stage (with multiple upstream branches) will
    be included even if only one of its upstreams is in the target's
    downstream chain. For reset/skip operations, use
    ``get_resettable_downstream_stages`` or ``get_skippable_downstream_stages``
    instead.

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


def get_resettable_downstream_stages(
    execution: Workflow,
    target_ref_id: str,
) -> list[StageExecution]:
    """Get downstream stages that can be safely reset for a backward jump.

    Unlike ``get_downstream_stages``, this function respects fan-in
    boundaries. A stage is only included if ALL of its prerequisite stages
    are within the reset scope (the target plus already-included stages).
    This prevents resetting fan-in stages that have upstream dependencies
    from branches not involved in the jump.

    Uses a fixed-point iteration: repeatedly scans all stages, adding those
    whose prerequisites are entirely within the accumulated scope, until no
    new stages are added.

    Args:
        execution: The workflow execution containing all stages
        target_ref_id: The ref_id of the jump target stage

    Returns:
        List of downstream stages safe to reset (excludes the target itself)
    """
    resettable_ref_ids: set[str] = {target_ref_id}
    resettable_stages: list[StageExecution] = []

    changed = True
    while changed:
        changed = False
        for stage in execution.stages:
            if stage.ref_id in resettable_ref_ids:
                continue
            prereqs = stage.requisite_stage_ref_ids
            if not prereqs:
                continue
            has_upstream_in_scope = any(r in resettable_ref_ids for r in prereqs)
            all_upstreams_in_scope = all(r in resettable_ref_ids for r in prereqs)
            if has_upstream_in_scope and all_upstreams_in_scope:
                resettable_ref_ids.add(stage.ref_id)
                resettable_stages.append(stage)
                changed = True

    return resettable_stages


def get_skippable_downstream_stages(
    execution: Workflow,
    source_ref_id: str,
) -> list[StageExecution]:
    """Get downstream stages that can be safely skipped for a forward jump.

    Like ``get_resettable_downstream_stages``, this respects fan-in
    boundaries. A stage is only considered skippable if ALL of its
    prerequisite stages are within the skip scope.

    Args:
        execution: The workflow execution containing all stages
        source_ref_id: The ref_id of the source stage (jumping from)

    Returns:
        List of downstream stages safe to skip (excludes the source itself)
    """
    skippable_ref_ids: set[str] = {source_ref_id}
    skippable_stages: list[StageExecution] = []

    changed = True
    while changed:
        changed = False
        for stage in execution.stages:
            if stage.ref_id in skippable_ref_ids:
                continue
            prereqs = stage.requisite_stage_ref_ids
            if not prereqs:
                continue
            has_upstream_in_scope = any(r in skippable_ref_ids for r in prereqs)
            all_upstreams_in_scope = all(r in skippable_ref_ids for r in prereqs)
            if has_upstream_in_scope and all_upstreams_in_scope:
                skippable_ref_ids.add(stage.ref_id)
                skippable_stages.append(stage)
                changed = True

    return skippable_stages


def get_skipped_stages(
    execution: Workflow,
    source_stage: StageExecution,
    target_stage: StageExecution,
) -> list[StageExecution]:
    """Get stages that should be skipped when jumping forward.

    These are stages that are exclusively downstream of the source
    (all their upstreams are in the source's downstream chain) but are
    NOT the target or in the target's downstream chain.

    Fan-in stages with upstream dependencies outside the source's
    downstream chain are automatically excluded.

    Args:
        execution: The workflow execution containing all stages
        source_stage: The stage jumping from
        target_stage: The stage jumping to

    Returns:
        List of stages to mark as SKIPPED
    """
    source_downstream = set(s.ref_id for s in get_skippable_downstream_stages(execution, source_stage.ref_id))

    # Get target and all stages downstream of target
    target_chain = {target_stage.ref_id}
    for s in get_downstream_stages(execution, target_stage.ref_id):
        target_chain.add(s.ref_id)

    # Skipped = downstream of source BUT NOT in target chain
    skipped_ref_ids = source_downstream - target_chain

    return [s for s in execution.stages if s.ref_id in skipped_ref_ids]
