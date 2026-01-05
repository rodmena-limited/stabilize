"""Tests for PythonTask."""

import tempfile
from pathlib import Path

from stabilize import PythonTask, StageExecution, TaskExecution, WorkflowStatus


class TestPythonTaskScript:
    """Test inline script execution."""

    def test_simple_script(self) -> None:
        """Test simple script without INPUT/RESULT."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Simple Script",
            context={"script": "print('hello world')"},
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert "hello world" in result.outputs["stdout"]
        assert result.outputs["exit_code"] == 0

    def test_script_with_input(self) -> None:
        """Test script accessing INPUT dict."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Script with Input",
            context={
                "script": "print(f'Name: {INPUT[\"name\"]}')",
                "inputs": {"name": "Alice"},
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert "Name: Alice" in result.outputs["stdout"]

    def test_script_with_result(self) -> None:
        """Test script setting RESULT variable."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Script with Result",
            context={
                "script": """
numbers = INPUT["numbers"]
RESULT = {"sum": sum(numbers), "count": len(numbers)}
""",
                "inputs": {"numbers": [1, 2, 3, 4, 5]},
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["result"] == {"sum": 15, "count": 5}

    def test_script_with_upstream_outputs(self) -> None:
        """Test that upstream outputs are available in INPUT."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Script with Upstream",
            context={
                "script": "RESULT = INPUT['upstream_value'] * 2",
                "upstream_value": 21,  # Simulates upstream output
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["result"] == 42


class TestPythonTaskScriptFile:
    """Test script file execution."""

    def test_script_file(self) -> None:
        """Test executing a script file."""
        task = PythonTask()

        # Create temp script file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
            tmp.write("RESULT = {'status': 'from_file'}\n")
            tmp.flush()
            script_path = tmp.name

        try:
            stage = StageExecution(
                ref_id="1",
                type="python",
                name="Script File",
                context={"script_file": script_path},
                tasks=[TaskExecution.create(name="test", implementing_class="python")],
            )

            result = task.execute(stage)

            assert result.status == WorkflowStatus.SUCCEEDED
            assert result.outputs["result"] == {"status": "from_file"}
        finally:
            Path(script_path).unlink(missing_ok=True)

    def test_script_file_not_found(self) -> None:
        """Test error when script file doesn't exist."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Missing File",
            context={"script_file": "/nonexistent/script.py"},
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "not found" in result.context["error"].lower()


class TestPythonTaskModule:
    """Test module + function execution."""

    def test_module_function(self) -> None:
        """Test calling a function from a module."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Module Function",
            context={
                "module": "json",
                "function": "dumps",
                "inputs": {"key": "value"},
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        # json.dumps({"key": "value"}) returns '{"key": "value"}'
        assert result.outputs["result"] == '{"key": "value"}'

    def test_module_without_function(self) -> None:
        """Test error when module specified without function."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Module No Function",
            context={"module": "json"},
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "function" in result.context["error"].lower()


class TestPythonTaskValidation:
    """Test input validation."""

    def test_no_execution_mode(self) -> None:
        """Test error when no execution mode specified."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="No Mode",
            context={},
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "script" in result.context["error"].lower() or "module" in result.context["error"].lower()

    def test_multiple_execution_modes(self) -> None:
        """Test error when multiple execution modes specified."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Multiple Modes",
            context={
                "script": "print('hello')",
                "script_file": "/some/file.py",
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "only one" in result.context["error"].lower()


class TestPythonTaskErrorHandling:
    """Test error handling."""

    def test_syntax_error(self) -> None:
        """Test handling of Python syntax errors."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Syntax Error",
            context={"script": "def broken("},
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert result.context["exit_code"] != 0

    def test_runtime_error(self) -> None:
        """Test handling of runtime errors."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Runtime Error",
            context={"script": "raise ValueError('test error')"},
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "ValueError" in result.context["stderr"]

    def test_continue_on_failure(self) -> None:
        """Test continue_on_failure flag."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Continue on Failure",
            context={
                "script": "import sys; sys.exit(1)",
                "continue_on_failure": True,
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.FAILED_CONTINUE
        assert result.outputs["exit_code"] == 1

    def test_timeout(self) -> None:
        """Test timeout handling."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Timeout",
            context={
                "script": "import time; time.sleep(10)",
                "timeout": 1,
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.TERMINAL
        assert "timed out" in result.context["error"].lower()


class TestPythonTaskExecution:
    """Test execution options."""

    def test_environment_variables(self) -> None:
        """Test passing environment variables."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Env Vars",
            context={
                "script": """
import os
RESULT = os.environ.get('MY_VAR')
""",
                "env": {"MY_VAR": "test_value"},
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["result"] == "test_value"

    def test_working_directory(self) -> None:
        """Test changing working directory."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Cwd",
            context={
                "script": """
import os
RESULT = os.getcwd()
""",
                "cwd": "/tmp",
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["result"] == "/tmp"

    def test_command_args(self) -> None:
        """Test passing command line arguments."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Args",
            context={
                "script": """
import sys
RESULT = sys.argv[1:]
""",
                "args": ["--flag", "value"],
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["result"] == ["--flag", "value"]


class TestPythonTaskIntegration:
    """Integration tests."""

    def test_data_processing_pipeline(self) -> None:
        """Test a realistic data processing scenario."""
        task = PythonTask()
        stage = StageExecution(
            ref_id="1",
            type="python",
            name="Data Processing",
            context={
                "script": """
data = INPUT["records"]
# Filter, transform, aggregate
valid = [r for r in data if r.get("active")]
totals = sum(r["value"] for r in valid)
RESULT = {
    "total_records": len(data),
    "active_records": len(valid),
    "total_value": totals,
}
""",
                "inputs": {
                    "records": [
                        {"id": 1, "value": 100, "active": True},
                        {"id": 2, "value": 200, "active": False},
                        {"id": 3, "value": 300, "active": True},
                    ]
                },
            },
            tasks=[TaskExecution.create(name="test", implementing_class="python")],
        )

        result = task.execute(stage)

        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.outputs["result"] == {
            "total_records": 3,
            "active_records": 2,
            "total_value": 400,
        }
