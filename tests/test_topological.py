"""Tests for topological sort algorithm."""

import pytest

from stabilize.dag.topological import (
    CircularDependencyError,
    find_initial_stages,
    find_terminal_stages,
    get_execution_layers,
    topological_sort,
)
from stabilize.models.stage import StageExecution
from stabilize.models.workflow import Workflow


def create_test_execution(*stages: StageExecution) -> Workflow:
    """Create a test execution with the given stages."""
    execution = Workflow(
        application="test",
        name="Test Pipeline",
        stages=list(stages),
    )
    return execution


def test_linear_pipeline() -> None:
    """Test linear pipeline: A -> B -> C"""
    execution = create_test_execution(
        StageExecution.create(type="stage", name="A", ref_id="1"),
        StageExecution.create(
            type="stage",
            name="B",
            ref_id="2",
            requisite_stage_ref_ids={"1"},
        ),
        StageExecution.create(
            type="stage",
            name="C",
            ref_id="3",
            requisite_stage_ref_ids={"2"},
        ),
    )

    sorted_stages = topological_sort(execution.stages)

    assert len(sorted_stages) == 3
    assert sorted_stages[0].ref_id == "1"
    assert sorted_stages[1].ref_id == "2"
    assert sorted_stages[2].ref_id == "3"


def test_parallel_branches() -> None:
    """Test parallel branches: A -> [B, C] -> D"""
    execution = create_test_execution(
        StageExecution.create(type="stage", name="A", ref_id="1"),
        StageExecution.create(
            type="stage",
            name="B",
            ref_id="2",
            requisite_stage_ref_ids={"1"},
        ),
        StageExecution.create(
            type="stage",
            name="C",
            ref_id="3",
            requisite_stage_ref_ids={"1"},
        ),
        StageExecution.create(
            type="stage",
            name="D",
            ref_id="4",
            requisite_stage_ref_ids={"2", "3"},
        ),
    )

    sorted_stages = topological_sort(execution.stages)

    assert len(sorted_stages) == 4
    # A must be first
    assert sorted_stages[0].ref_id == "1"
    # B and C can be in any order, but must be before D
    middle = {sorted_stages[1].ref_id, sorted_stages[2].ref_id}
    assert middle == {"2", "3"}
    # D must be last
    assert sorted_stages[3].ref_id == "4"


def test_diamond_pattern() -> None:
    """Test diamond pattern: A -> [B, C] -> D"""
    execution = create_test_execution(
        StageExecution.create(type="stage", name="A", ref_id="a"),
        StageExecution.create(
            type="stage",
            name="B",
            ref_id="b",
            requisite_stage_ref_ids={"a"},
        ),
        StageExecution.create(
            type="stage",
            name="C",
            ref_id="c",
            requisite_stage_ref_ids={"a"},
        ),
        StageExecution.create(
            type="stage",
            name="D",
            ref_id="d",
            requisite_stage_ref_ids={"b", "c"},
        ),
    )

    sorted_stages = topological_sort(execution.stages)

    # Verify order respects dependencies
    ref_ids = [s.ref_id for s in sorted_stages]
    assert ref_ids.index("a") < ref_ids.index("b")
    assert ref_ids.index("a") < ref_ids.index("c")
    assert ref_ids.index("b") < ref_ids.index("d")
    assert ref_ids.index("c") < ref_ids.index("d")


def test_multiple_initial_stages() -> None:
    """Test multiple initial stages: [A, B] -> C"""
    execution = create_test_execution(
        StageExecution.create(type="stage", name="A", ref_id="1"),
        StageExecution.create(type="stage", name="B", ref_id="2"),
        StageExecution.create(
            type="stage",
            name="C",
            ref_id="3",
            requisite_stage_ref_ids={"1", "2"},
        ),
    )

    sorted_stages = topological_sort(execution.stages)

    # A and B can be in any order
    assert sorted_stages[2].ref_id == "3"


def test_circular_dependency_detection() -> None:
    """Test that circular dependencies are detected."""
    execution = create_test_execution(
        StageExecution.create(
            type="stage",
            name="A",
            ref_id="1",
            requisite_stage_ref_ids={"2"},
        ),
        StageExecution.create(
            type="stage",
            name="B",
            ref_id="2",
            requisite_stage_ref_ids={"1"},
        ),
    )

    with pytest.raises(CircularDependencyError):
        topological_sort(execution.stages)


def test_complex_circular_dependency() -> None:
    """Test detection of more complex circular dependency."""
    execution = create_test_execution(
        StageExecution.create(
            type="stage",
            name="A",
            ref_id="1",
            requisite_stage_ref_ids={"3"},
        ),
        StageExecution.create(
            type="stage",
            name="B",
            ref_id="2",
            requisite_stage_ref_ids={"1"},
        ),
        StageExecution.create(
            type="stage",
            name="C",
            ref_id="3",
            requisite_stage_ref_ids={"2"},
        ),
    )

    with pytest.raises(CircularDependencyError):
        topological_sort(execution.stages)


def test_find_initial_stages() -> None:
    """Test finding initial stages."""
    execution = create_test_execution(
        StageExecution.create(type="stage", name="A", ref_id="1"),
        StageExecution.create(type="stage", name="B", ref_id="2"),
        StageExecution.create(
            type="stage",
            name="C",
            ref_id="3",
            requisite_stage_ref_ids={"1"},
        ),
    )

    initial = find_initial_stages(execution.stages)

    assert len(initial) == 2
    ref_ids = {s.ref_id for s in initial}
    assert ref_ids == {"1", "2"}


def test_find_terminal_stages() -> None:
    """Test finding terminal stages."""
    execution = create_test_execution(
        StageExecution.create(type="stage", name="A", ref_id="1"),
        StageExecution.create(
            type="stage",
            name="B",
            ref_id="2",
            requisite_stage_ref_ids={"1"},
        ),
        StageExecution.create(
            type="stage",
            name="C",
            ref_id="3",
            requisite_stage_ref_ids={"1"},
        ),
    )

    terminal = find_terminal_stages(execution.stages)

    assert len(terminal) == 2
    ref_ids = {s.ref_id for s in terminal}
    assert ref_ids == {"2", "3"}


def test_execution_layers() -> None:
    """Test grouping stages into execution layers."""
    execution = create_test_execution(
        StageExecution.create(type="stage", name="A", ref_id="1"),
        StageExecution.create(
            type="stage",
            name="B",
            ref_id="2",
            requisite_stage_ref_ids={"1"},
        ),
        StageExecution.create(
            type="stage",
            name="C",
            ref_id="3",
            requisite_stage_ref_ids={"1"},
        ),
        StageExecution.create(
            type="stage",
            name="D",
            ref_id="4",
            requisite_stage_ref_ids={"2", "3"},
        ),
    )

    layers = get_execution_layers(execution.stages)

    assert len(layers) == 3
    # Layer 0: A
    assert len(layers[0]) == 1
    assert layers[0][0].ref_id == "1"
    # Layer 1: B and C (parallel)
    assert len(layers[1]) == 2
    layer1_refs = {s.ref_id for s in layers[1]}
    assert layer1_refs == {"2", "3"}
    # Layer 2: D
    assert len(layers[2]) == 1
    assert layers[2][0].ref_id == "4"


def test_empty_pipeline() -> None:
    """Test handling of empty pipeline."""
    execution = create_test_execution()

    sorted_stages = topological_sort(execution.stages)
    assert sorted_stages == []

    initial = find_initial_stages(execution.stages)
    assert initial == []

    layers = get_execution_layers(execution.stages)
    assert layers == []
