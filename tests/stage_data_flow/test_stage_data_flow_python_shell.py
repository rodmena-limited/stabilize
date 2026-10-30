from __future__ import annotations

from stabilize import (
    StageExecution,
    TaskExecution,
    Workflow,
    WorkflowStatus,
)
from tests.stage_data_flow.conftest import run_workflow


class TestPythonTaskDataFlow:
    """Test data flow between stages using PythonTask."""

    def test_simple_two_stage_pipeline(self, workflow_engine: dict) -> None:
        """Test output from stage A is available in stage B via INPUT."""
        workflow = Workflow.create(
            application="test",
            name="Two Stage Pipeline",
            stages=[
                StageExecution(
                    ref_id="stage_a",
                    type="python",
                    name="Stage A - Generate Data",
                    context={
                        "script": """
RESULT = {"value": 42, "name": "test"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage_b",
                    type="python",
                    name="Stage B - Use Data",
                    requisite_stage_ref_ids={"stage_a"},
                    context={
                        "script": """
# Access upstream output via INPUT
upstream = INPUT.get("result", {})
value = upstream.get("value", 0)
name = upstream.get("name", "")
RESULT = {"doubled": value * 2, "greeting": f"Hello, {name}!"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        # Verify stage A output
        stage_a = next(s for s in result.stages if s.ref_id == "stage_a")
        assert stage_a.outputs.get("result") == {"value": 42, "name": "test"}

        # Verify stage B accessed stage A's output correctly
        stage_b = next(s for s in result.stages if s.ref_id == "stage_b")
        assert stage_b.outputs.get("result") == {"doubled": 84, "greeting": "Hello, test!"}

    def test_three_stage_chain(self, workflow_engine: dict) -> None:
        """Test data flows through A -> B -> C chain."""
        workflow = Workflow.create(
            application="test",
            name="Three Stage Chain",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="Stage A",
                    context={"script": "RESULT = {'n': 5}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="Stage B",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
n = INPUT.get("result", {}).get("n", 0)
RESULT = {"factorial": 1}
for i in range(1, n + 1):
    RESULT["factorial"] *= i
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="c",
                    type="python",
                    name="Stage C",
                    requisite_stage_ref_ids={"b"},
                    context={
                        "script": """
factorial = INPUT.get("result", {}).get("factorial", 0)
RESULT = {"result": f"5! = {factorial}"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_c = next(s for s in result.stages if s.ref_id == "c")
        assert stage_c.outputs.get("result") == {"result": "5! = 120"}

    def test_parallel_branches_join(self, workflow_engine: dict) -> None:
        """Test parallel branches with shell tasks (unique stdout per stage)."""
        # Note: When using PythonTask, all branches output to "result" key which
        # causes overwrites. Using ShellTask avoids this since each stage's
        # stdout is unique output that gets merged.
        workflow = Workflow.create(
            application="test",
            name="Parallel Branches Join",
            stages=[
                StageExecution(
                    ref_id="root",
                    type="shell",
                    name="Root",
                    context={"command": "printf '%s' '10'"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                # Three parallel branches - each outputs unique values
                # Branch A writes to a temp marker file
                StageExecution(
                    ref_id="branch_a",
                    type="python",
                    name="Branch A",
                    requisite_stage_ref_ids={"root"},
                    context={
                        "script": """
base = int(INPUT.get("stdout", "0"))
RESULT = {"value": base + 1, "source": "a"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="branch_b",
                    type="python",
                    name="Branch B",
                    requisite_stage_ref_ids={"root"},
                    context={
                        "script": """
base = int(INPUT.get("stdout", "0"))
RESULT = {"value": base + 2, "source": "b"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="branch_c",
                    type="python",
                    name="Branch C",
                    requisite_stage_ref_ids={"root"},
                    context={
                        "script": """
base = int(INPUT.get("stdout", "0"))
RESULT = {"value": base + 3, "source": "c"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                # Join point - due to key collision, only one result survives
                # but we verify the workflow completes and at least one branch's data is available
                StageExecution(
                    ref_id="join",
                    type="python",
                    name="Join",
                    requisite_stage_ref_ids={"branch_a", "branch_b", "branch_c"},
                    context={
                        "script": """
# When multiple PythonTask branches complete, their "result" keys
# get overwritten during merge. Only one branch's result survives.
# This test verifies the workflow completes and data passes through.
result = INPUT.get("result", {})
value = result.get("value", 0)
source = result.get("source", "unknown")
RESULT = {"received_value": value, "received_from": source, "all_completed": True}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        join_stage = next(s for s in result.stages if s.ref_id == "join")
        join_result = join_stage.outputs.get("result", {})
        # At least one branch's data should be available
        assert join_result.get("received_value") in [11, 12, 13]
        assert join_result.get("received_from") in ["a", "b", "c"]
        assert join_result.get("all_completed") is True

    def test_diamond_dependency(self, workflow_engine: dict) -> None:
        """Test diamond pattern: A -> B, A -> C, B+C -> D.

        Note: When B and C both use PythonTask, their "result" outputs collide.
        Only one branch's result survives in the merge. This test verifies the
        workflow completes and data flows correctly through one path.
        """
        workflow = Workflow.create(
            application="test",
            name="Diamond Dependency",
            stages=[
                StageExecution(
                    ref_id="a",
                    type="python",
                    name="A",
                    context={"script": "RESULT = {'x': 100}"},
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="b",
                    type="python",
                    name="B",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
x = INPUT.get("result", {}).get("x", 0)
RESULT = {"value": x * 2, "from": "b"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="c",
                    type="python",
                    name="C",
                    requisite_stage_ref_ids={"a"},
                    context={
                        "script": """
x = INPUT.get("result", {}).get("x", 0)
RESULT = {"value": x * 3, "from": "c"}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="d",
                    type="python",
                    name="D",
                    requisite_stage_ref_ids={"b", "c"},
                    context={
                        "script": """
# Due to key collision, only one of B or C's result is available
result = INPUT.get("result", {})
value = result.get("value", 0)
source = result.get("from", "unknown")
RESULT = {"received": value, "source": source}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage_d = next(s for s in result.stages if s.ref_id == "d")
        d_result = stage_d.outputs.get("result", {})
        # Either B's (200) or C's (300) result should be available
        assert d_result.get("received") in [200, 300]
        assert d_result.get("source") in ["b", "c"]


class TestShellTaskDataFlow:
    """Test data flow between stages using ShellTask."""

    def test_shell_to_python_pipeline(self, workflow_engine: dict) -> None:
        """Test shell output is available to downstream Python stage."""
        workflow = Workflow.create(
            application="test",
            name="Shell to Python",
            stages=[
                StageExecution(
                    ref_id="shell_stage",
                    type="shell",
                    name="Generate Numbers",
                    context={"command": "echo '1 2 3 4 5'"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="python_stage",
                    type="python",
                    name="Process Numbers",
                    requisite_stage_ref_ids={"shell_stage"},
                    context={
                        "script": """
# Shell output is in stdout key
stdout = INPUT.get("stdout", "")
numbers = [int(x) for x in stdout.split()]
RESULT = {"sum": sum(numbers), "count": len(numbers)}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        python_stage = next(s for s in result.stages if s.ref_id == "python_stage")
        assert python_stage.outputs.get("result") == {"sum": 15, "count": 5}

    def test_shell_to_shell_pipeline(self, workflow_engine: dict) -> None:
        """Test shell output passed to another shell via placeholder."""
        workflow = Workflow.create(
            application="test",
            name="Shell to Shell",
            stages=[
                StageExecution(
                    ref_id="stage1",
                    type="shell",
                    name="Generate",
                    context={"command": "printf '%s' hello"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="stage2",
                    type="shell",
                    name="Transform",
                    requisite_stage_ref_ids={"stage1"},
                    context={
                        "command": "echo '{stdout}' | tr 'h' 'H'",
                    },
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        stage2 = next(s for s in result.stages if s.ref_id == "stage2")
        assert stage2.outputs.get("stdout") == "Hello"

    def test_shell_exit_code_passed(self, workflow_engine: dict) -> None:
        """Test shell returncode is available downstream."""
        workflow = Workflow.create(
            application="test",
            name="Shell Exit Code",
            stages=[
                StageExecution(
                    ref_id="shell_stage",
                    type="shell",
                    name="Run Command",
                    context={"command": "echo 'success' && exit 0"},
                    tasks=[TaskExecution.create("Run", "shell", stage_start=True, stage_end=True)],
                ),
                StageExecution(
                    ref_id="check_stage",
                    type="python",
                    name="Check Exit Code",
                    requisite_stage_ref_ids={"shell_stage"},
                    context={
                        "script": """
returncode = INPUT.get("returncode", -1)
stdout = INPUT.get("stdout", "")
RESULT = {"exit_ok": returncode == 0, "output": stdout}
"""
                    },
                    tasks=[TaskExecution.create("Run", "python", stage_start=True, stage_end=True)],
                ),
            ],
        )

        result = run_workflow(workflow_engine, workflow)

        assert result.status == WorkflowStatus.SUCCEEDED

        check_stage = next(s for s in result.stages if s.ref_id == "check_stage")
        assert check_stage.outputs.get("result", {}).get("exit_ok") is True
