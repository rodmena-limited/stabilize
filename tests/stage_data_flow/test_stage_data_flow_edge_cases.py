from __future__ import annotations

from stabilize import (
    StageExecution,
    TaskExecution,
    Workflow,
    WorkflowStatus,
)
from tests.stage_data_flow.conftest import run_workflow


class TestEdgeCases:
    """Test edge cases for data flow."""

    def test_empty_result(self, workflow_engine: dict) -> None:
        """Test stage with empty RESULT passes through."""
        workflow = Workflow.create(
            application="test",
            name="Empty Result",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="Empty Output",
                    context={"script": "RESULT = {}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="Handle Empty",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
result = INPUT.get("result", {})
RESULT = {"empty": len(result) == 0}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_b = next(s for s in result.stages if s.ref_id == "b")
        assert stage_b.outputs.get("result", {}).get("empty") is True

    def test_complex_nested_result(self, workflow_engine: dict) -> None:
        """Test passing complex nested data structures."""
        workflow = Workflow.create(
            application="test",
            name="Complex Nested",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="Generate Complex",
                    context={
                        "script": """
RESULT = {
    "users": [
        {"name": "Alice", "age": 30, "tags": ["admin", "user"]},
        {"name": "Bob", "age": 25, "tags": ["user"]},
    ],
    "metadata": {
        "version": "1.0",
        "nested": {"deep": {"value": 42}},
    },
}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="Access Nested",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
result = INPUT.get("result", {})
users = result.get("users", [])
deep_value = result.get("metadata", {}).get("nested", {}).get("deep", {}).get("value", 0)
admin_count = sum(1 for u in users if "admin" in u.get("tags", []))
RESULT = {"user_count": len(users), "deep_value": deep_value, "admin_count": admin_count}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_b = next(s for s in result.stages if s.ref_id == "b")
        b_result = stage_b.outputs.get("result", {})
        assert b_result.get("user_count") == 2
        assert b_result.get("deep_value") == 42
        assert b_result.get("admin_count") == 1

    def test_no_upstream_dependency(self, workflow_engine: dict) -> None:
        """Test stages without dependencies run correctly."""
        workflow = Workflow.create(
            application="test",
            name="No Dependencies",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="Independent A",
                    context={"script": "RESULT = {'from': 'A'}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="Independent B",
                    context={"script": "RESULT = {'from': 'B'}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_a = next(s for s in result.stages if s.ref_id == "a")
        stage_b = next(s for s in result.stages if s.ref_id == "b")
        assert stage_a.outputs.get("result") == {"from": "A"}
        assert stage_b.outputs.get("result") == {"from": "B"}

    def test_transitive_dependencies(self, workflow_engine: dict) -> None:
        """Test that only direct dependencies are merged (not transitive)."""
        workflow = Workflow.create(
            application="test",
            name="Transitive Test",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="A",
                    context={"script": "RESULT = {'a_value': 'from_a'}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="B",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
# B depends on A, so a_value should be available
a_val = INPUT.get("result", {}).get("a_value", "missing")
RESULT = {"b_value": 'from_b', "saw_a": a_val}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="c",
                    type="python",
                    name="C",
                    requisite_stage_ref_ids={"b"},  # Only depends on B, not A
                    context={
                        "script": """
# C only depends on B directly
result = INPUT.get("result", {})
b_val = result.get("b_value", "missing")
saw_a = result.get("saw_a", "missing")
RESULT = {"c_saw_b": b_val, "c_saw_a_through_b": saw_a}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_c = next(s for s in result.stages if s.ref_id == "c")
        c_result = stage_c.outputs.get("result", {})
        assert c_result.get("c_saw_b") == "from_b"
        assert c_result.get("c_saw_a_through_b") == "from_a"


class TestDataFlowWithInputs:
    """Test data flow combined with explicit inputs."""

    def test_upstream_merged_with_inputs(self, workflow_engine: dict) -> None:
        """Test that upstream outputs are merged with explicit inputs."""
        workflow = Workflow.create(
            application="test",
            name="Merged Inputs",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="A",
                    context={"script": "RESULT = {'upstream': 100}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="B",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
# Both explicit inputs and upstream should be available
explicit = INPUT.get("explicit_value", 0)
upstream = INPUT.get("result", {}).get("upstream", 0)
RESULT = {"sum": explicit + upstream}
""",
                        "inputs": {"explicit_value": 50},
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_b = next(s for s in result.stages if s.ref_id == "b")
        assert stage_b.outputs.get("result", {}).get("sum") == 150
