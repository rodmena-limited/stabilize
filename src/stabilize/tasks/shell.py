"""
Built-in ShellTask for executing shell commands.

This module provides a ready-to-use ShellTask that:
- Executes shell commands via subprocess
- Substitutes {key} placeholders with context values (including upstream outputs)
- Returns stdout, stderr, and returncode as outputs
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


class ShellTask(Task):
    """
    Execute shell commands with placeholder substitution.

    This task reads a "command" from stage.context, substitutes any {key}
    placeholders with values from stage.context (including upstream outputs),
    and executes the command.

    Context Parameters:
        command: The shell command to execute (required)
        timeout: Command timeout in seconds (default: 60)
        continue_on_failure: If True, return failed_continue instead of terminal on error

    Outputs:
        stdout: Command standard output (stripped)
        stderr: Command standard error (stripped)
        returncode: Command exit code

    Placeholder Substitution:
        Any {key} in the command is replaced with the value of stage.context[key].
        This includes outputs from upstream stages.

    Example:
        # Stage 1 outputs {"result": "hello"}
        # Stage 2 context: {"command": "echo '{result}'"}
        # After substitution: "echo 'hello'"

    Usage:
        from stabilize.tasks.shell import ShellTask

        registry = TaskRegistry()
        registry.register("shell", ShellTask)

        StageExecution(
            ref_id="2",
            type="shell",
            requisite_stage_ref_ids={"1"},  # Wait for stage 1
            context={"command": "echo '{stdout}' > /tmp/out.txt"},
            ...
        )
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        command = stage.context.get("command")
        if not command:
            return TaskResult.terminal(error="No 'command' specified in context")

        timeout = stage.context.get("timeout", 60)
        continue_on_failure = stage.context.get("continue_on_failure", False)

        # Substitute {key} placeholders with context values (includes upstream outputs)
        for key, value in stage.context.items():
            if key not in ("command", "timeout", "continue_on_failure"):
                if isinstance(value, str):
                    command = command.replace("{" + key + "}", value)
                elif value is not None:
                    command = command.replace("{" + key + "}", str(value))

        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            outputs = {
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
                "returncode": result.returncode,
            }

            if result.returncode != 0:
                error_msg = f"Command failed with exit code {result.returncode}: {result.stderr.strip()}"
                if continue_on_failure:
                    return TaskResult.failed_continue(error=error_msg, outputs=outputs)
                return TaskResult.terminal(error=error_msg, context=outputs)

            return TaskResult.success(outputs=outputs)

        except subprocess.TimeoutExpired:
            return TaskResult.terminal(error=f"Command timed out after {timeout}s")
        except Exception as e:
            return TaskResult.terminal(error=str(e))
