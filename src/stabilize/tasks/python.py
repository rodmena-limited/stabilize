"""
PythonTask for executing Python code in isolated subprocess.

This module provides a production-ready PythonTask with:
- Inline script execution
- Script file execution
- Module + function execution
- INPUT/RESULT variable convention for data passing
- Full subprocess isolation with timeout support
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)


class PythonTask(Task):
    """
        Execute Python code in isolated subprocess.

        Supports three execution modes:
        1. Inline script: Pass Python code as a string
        2. Script file: Load and execute a Python file
        3. Module + function: Import a module and call a function

        All modes run in a subprocess for isolation and hard timeout enforcement.

        Context Parameters:
            # Script execution (choose one):
            script (str): Inline Python code to execute
            script_file (str): Path to Python script file
            module (str): Python module path (e.g., "myapp.tasks.validate")
            function (str): Function name to call (requires module)

            # Inputs:
            inputs (dict): Input variables, available as INPUT in script (optional)
            args (list): Command line arguments (optional)

            # Execution:
            python_path (str): Python interpreter path (default: current interpreter)
            timeout (int): Execution timeout in seconds (default: 60)
            cwd (str): Working directory (optional)
            env (dict): Environment variables to add (optional)
            continue_on_failure (bool): Return failed_continue on error (default: False)

        Outputs:
            stdout (str): Standard output from script
            stderr (str): Standard error from script
            exit_code (int): Process exit code
            result (any): Value of RESULT variable if set in script

        Notes:
            - Scripts access input data via the INPUT dict
            - Scripts set return value via the RESULT variable
            - RESULT must be JSON-serializable
            - Module mode: imports module.function and calls with INPUT as argument
            - Upstream stage outputs are automatically available in INPUT

        Examples:
            # Inline script
            context = {
                "script": '''
    result = sum(INPUT["numbers"])
    RESULT = {"sum": result, "count": len(INPUT["numbers"])}
    ''',
                "inputs": {"numbers": [1, 2, 3, 4, 5]}
            }

            # Script file
            context = {
                "script_file": "/path/to/script.py",
                "inputs": {"config": {"debug": True}}
            }

            # Module + function
            context = {
                "module": "myapp.validators",
                "function": "validate_input",
                "inputs": {"data": {"name": "test"}}
            }
    """

    # Wrapper template for script mode - handles INPUT/RESULT
    SCRIPT_WRAPPER = """\
import json
import sys

# Input data from upstream stages and explicit inputs
try:
    INPUT = json.loads('''{inputs_json}''')
except json.JSONDecodeError:
    INPUT = {{}}

# User script
{script}

# Output result if RESULT variable was set
if 'RESULT' in dir():
    print("__PYTHONTASK_RESULT_START__")
    print(json.dumps(RESULT))
    print("__PYTHONTASK_RESULT_END__")
"""

    # Wrapper template for module+function mode
    MODULE_WRAPPER = """\
import json
import sys

# Input data
try:
    INPUT = json.loads('''{inputs_json}''')
except json.JSONDecodeError:
    INPUT = {{}}

# Import and call function
from {module} import {function}
RESULT = {function}(INPUT)

# Output result
print("__PYTHONTASK_RESULT_START__")
print(json.dumps(RESULT))
print("__PYTHONTASK_RESULT_END__")
"""

    RESULT_START_MARKER = "__PYTHONTASK_RESULT_START__"
    RESULT_END_MARKER = "__PYTHONTASK_RESULT_END__"

    def execute(self, stage: StageExecution) -> TaskResult:
        """Execute Python code based on context parameters."""
        # Get execution mode parameters
        script = stage.context.get("script")
        script_file = stage.context.get("script_file")
        module = stage.context.get("module")
        function = stage.context.get("function")

        # Validate execution mode
        mode_count = sum(bool(x) for x in [script, script_file, module])
        if mode_count == 0:
            return TaskResult.terminal(error="One of 'script', 'script_file', or 'module' must be specified")
        if mode_count > 1:
            return TaskResult.terminal(error="Only one of 'script', 'script_file', or 'module' can be specified")
        if module and not function:
            return TaskResult.terminal(error="'function' is required when using 'module' mode")

        # Get execution parameters
        args = stage.context.get("args", [])
        python_path = stage.context.get("python_path", sys.executable)
        timeout = stage.context.get("timeout", 60)
        cwd = stage.context.get("cwd")
        env_vars = stage.context.get("env", {})
        continue_on_failure = stage.context.get("continue_on_failure", False)

        # Build inputs: merge ancestor outputs + stage context + explicit inputs
        # This allows upstream stage outputs to flow to downstream Python tasks
        # Exclude internal parameters from being passed as inputs
        internal_keys = {
            "script",
            "script_file",
            "module",
            "function",
            "args",
            "python_path",
            "timeout",
            "cwd",
            "env",
            "continue_on_failure",
            "inputs",
        }

        # Start with ancestor outputs (upstream stage data)
        ancestor_outputs: dict[str, Any] = {}
        try:
            for ancestor in reversed(stage.ancestors()):
                if ancestor.outputs:
                    ancestor_outputs.update(ancestor.outputs)
        except (AttributeError, ValueError):
            # Stage might not be attached to an execution yet
            pass

        # Filter out internal keys from context
        base_inputs = {k: v for k, v in stage.context.items() if k not in internal_keys}

        # Merge: ancestor outputs -> stage context -> explicit inputs
        explicit_inputs = stage.context.get("inputs", {})
        inputs = {**ancestor_outputs, **base_inputs, **explicit_inputs}

        # Generate wrapped script
        try:
            wrapped_script = self._generate_script(
                script=script,
                script_file=script_file,
                module=module,
                function=function,
                inputs=inputs,
            )
        except Exception as e:
            return TaskResult.terminal(error=f"Failed to generate script: {e}")

        # Write to temp file
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as tmp:
                tmp.write(wrapped_script)
                tmp.flush()
                tmp_path = tmp.name

            # Build command
            cmd = [python_path, tmp_path] + list(args)

            # Build environment
            env = os.environ.copy()
            env.update(env_vars)

            logger.debug(f"PythonTask executing: {' '.join(cmd)}")

            # Execute
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=cwd,
                env=env,
            )

            # Parse output
            stdout = result.stdout
            stderr = result.stderr
            exit_code = result.returncode

            # Extract RESULT if present
            script_result = self._extract_result(stdout)

            # Clean stdout (remove result markers)
            clean_stdout = self._clean_stdout(stdout)

            outputs: dict[str, Any] = {
                "stdout": clean_stdout,
                "stderr": stderr,
                "exit_code": exit_code,
            }

            if script_result is not None:
                outputs["result"] = script_result

            if exit_code == 0:
                logger.debug(f"PythonTask success, result: {str(script_result)[:100]}")
                return TaskResult.success(outputs=outputs)
            else:
                error_msg = f"Python script failed with exit code {exit_code}"
                if stderr:
                    error_msg += f": {stderr[:200]}"
                if continue_on_failure:
                    return TaskResult.failed_continue(error=error_msg, outputs=outputs)
                return TaskResult.terminal(error=error_msg, context=outputs)

        except subprocess.TimeoutExpired:
            error_msg = f"Python script timed out after {timeout}s"
            if continue_on_failure:
                return TaskResult.failed_continue(error=error_msg)
            return TaskResult.terminal(error=error_msg)

        except Exception as e:
            error_msg = f"Python execution failed: {e}"
            if continue_on_failure:
                return TaskResult.failed_continue(error=error_msg)
            return TaskResult.terminal(error=error_msg)

        finally:
            # Clean up temp file
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    def _generate_script(
        self,
        script: str | None,
        script_file: str | None,
        module: str | None,
        function: str | None,
        inputs: dict[str, Any],
    ) -> str:
        """Generate the wrapped script to execute."""
        # Double-escape backslashes so they survive embedding in triple-quoted strings.
        # Without this, "\n" in JSON becomes an actual newline in the Python code,
        # causing json.loads to fail with JSONDecodeError.
        inputs_json = json.dumps(inputs).replace("\\", "\\\\")

        if module and function:
            # Module + function mode
            return self.MODULE_WRAPPER.format(
                inputs_json=inputs_json,
                module=module,
                function=function,
            )

        # Script mode (inline or file)
        if script_file:
            script_path = Path(script_file)
            if not script_path.exists():
                raise FileNotFoundError(f"Script file not found: {script_file}")
            script = script_path.read_text()

        assert script is not None
        return self.SCRIPT_WRAPPER.format(
            inputs_json=inputs_json,
            script=script,
        )

    def _extract_result(self, stdout: str) -> Any:
        """Extract RESULT value from stdout if present."""
        if self.RESULT_START_MARKER not in stdout:
            return None

        try:
            start = stdout.index(self.RESULT_START_MARKER) + len(self.RESULT_START_MARKER)
            end = stdout.index(self.RESULT_END_MARKER)
            result_json = stdout[start:end].strip()
            return json.loads(result_json)
        except (ValueError, json.JSONDecodeError):
            return None

    def _clean_stdout(self, stdout: str) -> str:
        """Remove result markers from stdout."""
        if self.RESULT_START_MARKER not in stdout:
            return stdout.strip()

        try:
            start = stdout.index(self.RESULT_START_MARKER)
            end = stdout.index(self.RESULT_END_MARKER) + len(self.RESULT_END_MARKER)
            clean = stdout[:start] + stdout[end:]
            return clean.strip()
        except ValueError:
            return stdout.strip()
