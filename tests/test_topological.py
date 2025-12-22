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
