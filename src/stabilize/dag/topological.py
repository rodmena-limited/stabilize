from __future__ import annotations
from collections.abc import Callable
from typing import TYPE_CHECKING

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
    # Filter stages by predicate
    unsorted: list[StageExecution] = [s for s in stages if stage_filter(s)]
    sorted_stages: list[StageExecution] = []
    ref_ids: set[str] = set()

    while unsorted:
        # Find all stages whose requisites have been satisfied
        # A stage is sortable if all its requisite_stage_ref_ids are in ref_ids
        sortable = [stage for stage in unsorted if ref_ids.issuperset(stage.requisite_stage_ref_ids)]

        if not sortable:
            # No progress possible - circular dependency
            relationships = ", ".join(f"{list(stage.requisite_stage_ref_ids)}->{stage.ref_id}" for stage in stages)
            raise CircularDependencyError(
                f"Invalid stage relationships found: {relationships}",
                stages=unsorted,
            )

        # Add all sortable stages to result
        for stage in sortable:
            unsorted.remove(stage)
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

class CircularDependencyError(Exception):
    """
    Raised when a circular dependency is detected in the stage graph.

    This indicates an invalid pipeline configuration where stages depend
    on each other in a cycle.
    """
    def __init__(self, message: str, stages: list[StageExecution] | None = None):
        super().__init__(message)
        self.stages = stages or []
