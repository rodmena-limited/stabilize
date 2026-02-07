"""
Topological sort for stage execution ordering.

This module implements a topological sort algorithm for stages based on their
requisite_stage_ref_ids (DAG edges).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


class CircularDependencyError(Exception):
    """
    Raised when a circular dependency is detected in the stage graph.

    This indicates an invalid pipeline configuration where stages depend
    on each other in a cycle.
    """

    def __init__(self, message: str, stages: list[StageExecution] | None = None):
        super().__init__(message)
        self.stages = stages or []


def topological_sort(
    stages: list[StageExecution],
    stage_filter: Callable[[StageExecution], bool] = lambda s: s.parent_stage_id is None,
) -> list[StageExecution]:
    """
    Sort stages into topological order based on their dependencies.

    The algorithm:

    1. Starts with all unsorted stages (filtered by predicate)
    2. Finds stages whose requisites are all in the "processed" set
    3. Adds those stages to result and their ref_ids to processed set
    4. Repeats until all stages are sorted
    5. Raises CircularDependencyError if no progress can be made

    Args:
        stages: List of stages to sort
        stage_filter: Predicate to filter stages (default: exclude synthetic stages)

    Returns:
        List of stages sorted in execution order

    Raises:
        CircularDependencyError: If stages have circular dependencies

    Example:
        # Linear: A -> B -> C
        stages = [stage_c, stage_a, stage_b]
        sorted_stages = topological_sort(stages)
        # Result: [stage_a, stage_b, stage_c]

        # Parallel with join: A -> [B, C] -> D
        # B and C have requisites [A], D has requisites [B, C]
        sorted_stages = topological_sort(stages)
        # Result: [A, B, C, D] or [A, C, B, D] (B and C can be in any order)
    """
    # Filter stages by predicate, build a map for O(1) lookup
    filtered_stages = [s for s in stages if stage_filter(s)]
    # Use set of stage ids for O(1) removal tracking
    unsorted_ids: set[str] = {s.id for s in filtered_stages}
    # Map from id to stage for quick lookup
    stage_by_id: dict[str, StageExecution] = {s.id: s for s in filtered_stages}

    sorted_stages: list[StageExecution] = []
    ref_ids: set[str] = set()

    while unsorted_ids:
        # Find all stages whose requisites have been satisfied
        # A stage is sortable if all its requisite_stage_ref_ids are in ref_ids
        sortable = [
            stage_by_id[sid]
            for sid in unsorted_ids
            if ref_ids.issuperset(stage_by_id[sid].requisite_stage_ref_ids)
        ]

        if not sortable:
            # No progress possible - circular dependency
            unsorted_stages = [stage_by_id[sid] for sid in unsorted_ids]
            relationships = ", ".join(
                f"{list(stage.requisite_stage_ref_ids)}->{stage.ref_id}"
                for stage in unsorted_stages
            )
            raise CircularDependencyError(
                f"Invalid stage relationships found: {relationships}",
                stages=unsorted_stages,
            )

        # Add all sortable stages to result (O(1) removal from set)
        for stage in sortable:
            unsorted_ids.remove(stage.id)
            ref_ids.add(stage.ref_id)
            sorted_stages.append(stage)

    return sorted_stages


def topological_sort_all_stages(stages: list[StageExecution]) -> list[StageExecution]:
    """
    Sort all stages including synthetic stages.

    Unlike topological_sort(), this does not filter out synthetic stages.

    Args:
        stages: List of stages to sort

    Returns:
        List of all stages sorted in execution order
    """
    return topological_sort(stages, stage_filter=lambda s: True)


def validate_dag(stages: list[StageExecution]) -> bool:
    """
    Validate that stages form a valid DAG.

    Returns True if valid, raises CircularDependencyError if invalid.

    Args:
        stages: List of stages to validate

    Returns:
        True if DAG is valid

    Raises:
        CircularDependencyError: If stages have circular dependencies
    """
    topological_sort(stages)
    return True


def find_initial_stages(stages: list[StageExecution]) -> list[StageExecution]:
    """
    Find all initial stages (those with no dependencies and not synthetic).

    Args:
        stages: List of stages to search

    Returns:
        List of initial stages
    """
    return [stage for stage in stages if stage.is_initial() and not stage.is_synthetic()]


def find_terminal_stages(stages: list[StageExecution]) -> list[StageExecution]:
    """
    Find all terminal stages (those with no downstream stages and not synthetic).

    Terminal stages are the last stages in the pipeline - no other stages
    depend on them.

    Args:
        stages: List of stages to search

    Returns:
        List of terminal stages
    """
    # Get all ref_ids that are dependencies
    all_requisites: set[str] = set()
    for stage in stages:
        all_requisites.update(stage.requisite_stage_ref_ids)

    # Terminal stages are those whose ref_id is not in any requisite set
    return [
        stage
        for stage in stages
        if not stage.is_synthetic() and stage.ref_id not in all_requisites
    ]


def get_execution_layers(stages: list[StageExecution]) -> list[list[StageExecution]]:
    """
    Group stages into execution layers.

    Stages in the same layer can execute in parallel.
    Each layer depends only on stages in previous layers.

    Args:
        stages: List of stages to group

    Returns:
        List of layers, where each layer is a list of stages that can run in parallel

    Example:
        # A -> [B, C] -> D
        # Layer 0: [A]
        # Layer 1: [B, C]
        # Layer 2: [D]
    """
    # Filter and build maps for O(1) operations
    filtered_stages = [s for s in stages if s.parent_stage_id is None]
    unsorted_ids: set[str] = {s.id for s in filtered_stages}
    stage_by_id: dict[str, StageExecution] = {s.id: s for s in filtered_stages}

    layers: list[list[StageExecution]] = []
    ref_ids: set[str] = set()

    while unsorted_ids:
        # Find all stages whose requisites are satisfied
        layer = [
            stage_by_id[sid]
            for sid in unsorted_ids
            if ref_ids.issuperset(stage_by_id[sid].requisite_stage_ref_ids)
        ]

        if not layer:
            # This shouldn't happen if topological_sort passes
            break

        layers.append(layer)

        # O(1) removal from set
        for stage in layer:
            unsorted_ids.remove(stage.id)
            ref_ids.add(stage.ref_id)

    return layers
