import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

class PythonTask(Task):
    """
    Execute Python code.

    Context Parameters:
        script: Inline Python code to execute (string)
        script_file: Path to Python script file (alternative to script)
        args: Command line arguments as list (optional)
        inputs: Input variables as dict, available as INPUT in script (optional)
        python_path: Python interpreter path (default: current interpreter)
        timeout: Execution timeout in seconds (default: 60)

    Outputs:
        stdout: Standard output
        stderr: Standard error
        exit_code: Process exit code
        result: Value of RESULT variable if set in script

    Notes:
        - Scripts can access INPUT dict for inputs
        - Scripts should set RESULT variable for return value
        - RESULT must be JSON-serializable
    """
    WRAPPER_TEMPLATE = '\nimport json\nimport sys\n\n# Input data\ntry:\n    INPUT = json.loads(\'\'\'{inputs}\'\'\')\nexcept json.JSONDecodeError:\n    # Fallback for simple cases or empty\n    INPUT = {{}}\n\n# User script\n{script}\n\n# Output result if set\nif \'RESULT\' in dir():\n    print("__RESULT_START__")\n    print(json.dumps(RESULT))\n    print("__RESULT_END__")\n'

    def execute(self, stage: StageExecution) -> TaskResult:
        script = stage.context.get("script")
        script_file = stage.context.get("script_file")
        args = stage.context.get("args", [])

        # Merge inputs with stage context (which contains upstream outputs)
        # 1. Start with stage context (excludes script, args, etc to keep it clean?)
        # 2. Update with explicit inputs
        base_context = {
            k: v
            for k, v in stage.context.items()
            if k not in ("script", "script_file", "args", "inputs", "python_path", "timeout")
        }
        explicit_inputs = stage.context.get("inputs", {})
        inputs = {**base_context, **explicit_inputs}

        python_path = stage.context.get("python_path", sys.executable)
        timeout = stage.context.get("timeout", 60)

        if not script and not script_file:
            return TaskResult.terminal(error="Either 'script' or 'script_file' must be specified")

        if script and script_file:
            return TaskResult.terminal(error="Cannot specify both 'script' and 'script_file'")

        # Handle script file
        if script_file:
            script_path = Path(script_file)
            if not script_path.exists():
                return TaskResult.terminal(error=f"Script file not found: {script_file}")
            script = script_path.read_text()

        # At this point, script is guaranteed to be a string (validated above)
        assert script is not None
        print(f"  [PythonTask] Executing script ({len(script)} chars)")

        # Create wrapped script
        wrapped_script = self.WRAPPER_TEMPLATE.format(
            inputs=json.dumps(inputs),
            script=script,
        )

        # Write to temp file and execute
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
            tmp.write(wrapped_script)
            tmp.flush()
            tmp_path = tmp.name

        try:
            cmd = [python_path, tmp_path] + list(args)
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            stdout = result.stdout
            stderr = result.stderr
            exit_code = result.returncode

            # Extract RESULT if present
            script_result = None
            if "__RESULT_START__" in stdout:
                start = stdout.index("__RESULT_START__") + len("__RESULT_START__\n")
                end = stdout.index("__RESULT_END__")
                result_json = stdout[start:end].strip()
                try:
                    script_result = json.loads(result_json)
                except json.JSONDecodeError:
                    script_result = result_json

                # Clean stdout
                stdout = (
                    stdout[: stdout.index("__RESULT_START__")]
                    + stdout[stdout.index("__RESULT_END__") + len("__RESULT_END__\n") :]
                ).strip()

            outputs = {
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": exit_code,
                "result": script_result,
            }

            if exit_code == 0:
                print(f"  [PythonTask] Success, result: {str(script_result)[:100]}")
                return TaskResult.success(outputs=outputs)
            else:
                print(f"  [PythonTask] Failed with exit code {exit_code}")
                if stderr:
                    print(f"  [PythonTask] Stderr: {stderr}")
                return TaskResult.terminal(
                    error=f"Script exited with code {exit_code}",
                    context=outputs,
                )

        except subprocess.TimeoutExpired:
            return TaskResult.terminal(error=f"Script timed out after {timeout}s")

        finally:
            Path(tmp_path).unlink(missing_ok=True)
