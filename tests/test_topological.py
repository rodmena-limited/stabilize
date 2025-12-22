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
