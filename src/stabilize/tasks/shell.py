"""
Enterprise-ready ShellTask for executing shell commands.

This module provides a production-ready ShellTask with:
- Working directory support
- Environment variable injection
- Shell selection (bash, sh, custom)
- Stdin input support
- Output size limits (prevent OOM)
- Expected exit codes (for tools that return non-zero on success)
- Secret masking in logs
- Binary output mode
- {key} placeholder substitution with upstream outputs
- Process group cleanup on timeout
"""

from __future__ import annotations

import base64
import ctypes
import logging
import os
import shlex
import signal
import subprocess
import sys
from typing import TYPE_CHECKING, Any

from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)

# Keys that should not be substituted as placeholders
RESERVED_KEYS = frozenset(
    {
        "command",
        "timeout",
        "cwd",
        "env",
        "shell",
        "stdin",
        "max_output_size",
        "expected_codes",
        "secrets",
        "binary",
        "continue_on_failure",
    }
)


class ShellTask(Task):
    """
    Enterprise-ready shell command execution.

    Executes shell commands with full control over execution environment,
    input/output handling, and error management.

    Context Parameters:
        command (str): The shell command to execute (required)
        timeout (int): Command timeout in seconds (default: 60)
        cwd (str): Working directory for command execution
        env (dict): Additional environment variables to set
        shell (bool|str): True for default shell, or path to shell executable
        stdin (str): Input to send to command's stdin
        max_output_size (int): Max bytes for stdout/stderr (default: 10MB)
        expected_codes (list[int]): Exit codes to treat as success (default: [0])
        secrets (list[str]): Context keys whose values should be masked in logs
        binary (bool): If True, capture output as bytes (default: False)
        continue_on_failure (bool): If True, return failed_continue instead of terminal

    Outputs:
        stdout (str|bytes): Command standard output (stripped if text mode)
        stderr (str|bytes): Command standard error (stripped if text mode)
        returncode (int): Command exit code
        truncated (bool): True if output was truncated due to size limit
        stdout_b64 (str): Base64-encoded stdout (only if binary=True)

    Placeholder Substitution:
        Any {key} in the command is replaced with stage.context[key].
        This includes outputs from upstream stages.

    Examples:
        # Basic command
        context={"command": "ls -la"}

        # With working directory
        context={"command": "npm install", "cwd": "/app/frontend"}

        # With environment variables
        context={"command": "./deploy.sh", "env": {"AWS_REGION": "us-east-1"}}

        # With custom shell
        context={"command": "source venv/bin/activate && pytest", "shell": "/bin/bash"}

        # With stdin input
        context={"command": "cat", "stdin": "Hello World"}

        # Allow grep's exit code 1 (no match)
        context={"command": "grep pattern file.txt", "expected_codes": [0, 1]}

        # Mask secrets in logs
        context={
            "command": "curl -H 'Authorization: Bearer {token}' https://api.example.com",
            "token": "secret123",
            "secrets": ["token"]
        }

        # Binary output
        context={"command": "cat image.png", "binary": True}

        # Using upstream output
        context={"command": "echo 'Previous output: {stdout}'"}
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        """Execute the shell command with all configured options."""
        command = stage.context.get("command")
        if not command:
            return TaskResult.terminal(error="No 'command' specified in context")

        # Extract options with defaults
        timeout: int = stage.context.get("timeout", 60)
        cwd: str | None = stage.context.get("cwd")
        env: dict[str, str] = stage.context.get("env", {})
        shell: bool | str = stage.context.get("shell", True)
        stdin_input: str | None = stage.context.get("stdin")
        max_output_size: int = stage.context.get("max_output_size", 10 * 1024 * 1024)
        expected_codes: list[int] = stage.context.get("expected_codes", [0])
        secrets: list[str] = stage.context.get("secrets", [])
        binary: bool = stage.context.get("binary", False)
        continue_on_failure: bool = stage.context.get("continue_on_failure", False)

        # Substitute {key} placeholders with context values (includes upstream outputs)
        for key, value in stage.context.items():
            if key not in RESERVED_KEYS:
                placeholder = "{" + key + "}"
                if placeholder in command:
                    if isinstance(value, str):
                        # Quote value to prevent injection
                        quoted_value = shlex.quote(value)
                        command = command.replace(placeholder, quoted_value)
                    elif value is not None:
                        # Non-strings are converted but also quoted if they might contain dangerous chars?
                        # Generally numbers are safe, but objects str() repr might not be.
                        # Safer to quote everything.
                        quoted_value = shlex.quote(str(value))
                        command = command.replace(placeholder, quoted_value)

        # Build full environment (inherit + custom)
        full_env = os.environ.copy()
        for k, v in env.items():
            full_env[k] = str(v) if not isinstance(v, str) else v

        # Log command with secrets masked
        log_command = command
        for secret_key in secrets:
            secret_value = stage.context.get(secret_key)
            if secret_value is not None:
                log_command = log_command.replace(str(secret_value), "***")

        logger.debug("ShellTask executing: %s", log_command)

        try:
            # Build subprocess arguments for Popen
            popen_kwargs: dict[str, Any] = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.PIPE,
                "start_new_session": True,  # Enable process group for cleanup
            }

            # Handle shell option
            if isinstance(shell, str):
                # Custom shell path
                popen_kwargs["shell"] = True
                popen_kwargs["executable"] = shell
            else:
                popen_kwargs["shell"] = shell

            # Handle stdin - must use PIPE for input
            if stdin_input is not None:
                popen_kwargs["stdin"] = subprocess.PIPE

            # Optional arguments
            if cwd:
                popen_kwargs["cwd"] = cwd
            if full_env:
                popen_kwargs["env"] = full_env

            # Execute command using Popen for process group management
            proc = subprocess.Popen(command, **popen_kwargs)

            try:
                # Communicate with timeout
                stdin_bytes = None
                if stdin_input is not None:
                    stdin_bytes = stdin_input if binary else stdin_input.encode()

                stdout_bytes, stderr_bytes = proc.communicate(input=stdin_bytes, timeout=timeout)

            except subprocess.TimeoutExpired:
                # Kill entire process group on timeout
                self._kill_process_group(proc)
                # Collect any partial output
                try:
                    stdout_bytes, stderr_bytes = proc.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    stdout_bytes = b""
                    stderr_bytes = b""
                    proc.kill()
                    proc.wait()

                timeout_outputs: dict[str, Any] = {"returncode": -1, "truncated": False}
                if stdout_bytes:
                    if len(stdout_bytes) > max_output_size:
                        stdout_bytes = stdout_bytes[:max_output_size]
                    if binary:
                        timeout_outputs["stdout"] = stdout_bytes
                    else:
                        timeout_outputs["stdout"] = stdout_bytes.decode("utf-8", errors="replace").strip()
                if stderr_bytes:
                    if len(stderr_bytes) > max_output_size:
                        stderr_bytes = stderr_bytes[:max_output_size]
                    if binary:
                        timeout_outputs["stderr"] = stderr_bytes
                    else:
                        timeout_outputs["stderr"] = stderr_bytes.decode("utf-8", errors="replace").strip()

                error_msg = f"Command timed out after {timeout}s"
                if continue_on_failure:
                    return TaskResult.failed_continue(error=error_msg, outputs=timeout_outputs)
                return TaskResult.terminal(error=error_msg, context=timeout_outputs)

            # Process output with size limits
            truncated = False

            if len(stdout_bytes) > max_output_size:
                stdout_bytes = stdout_bytes[:max_output_size]
                truncated = True
            if len(stderr_bytes) > max_output_size:
                stderr_bytes = stderr_bytes[:max_output_size]
                truncated = True

            # Build outputs
            outputs: dict[str, Any] = {
                "returncode": proc.returncode,
                "truncated": truncated,
            }

            if binary:
                outputs["stdout"] = stdout_bytes
                outputs["stderr"] = stderr_bytes
                outputs["stdout_b64"] = base64.b64encode(stdout_bytes).decode("ascii")
            else:
                outputs["stdout"] = stdout_bytes.decode("utf-8", errors="replace").strip() if stdout_bytes else ""
                outputs["stderr"] = stderr_bytes.decode("utf-8", errors="replace").strip() if stderr_bytes else ""

            # Check exit code
            if proc.returncode in expected_codes:
                return TaskResult.success(outputs=outputs)
            else:
                error_msg = f"Command exited with code {proc.returncode} (expected: {expected_codes})"
                if outputs.get("stderr"):
                    error_msg += f": {outputs['stderr'][:200]}"

                if continue_on_failure:
                    return TaskResult.failed_continue(error=error_msg, outputs=outputs)
                return TaskResult.terminal(error=error_msg, context=outputs)

        except FileNotFoundError as e:
            return TaskResult.terminal(error=f"Command or shell not found: {e}")

        except PermissionError as e:
            return TaskResult.terminal(error=f"Permission denied: {e}")

        except OSError as e:
            return TaskResult.terminal(error=f"OS error: {e}")

        except Exception as e:
            return TaskResult.terminal(error=f"Unexpected error: {e}")

    def _kill_process_group(self, proc: subprocess.Popen[Any]) -> None:
        """Kill the entire process group for cleanup on timeout.

        Uses SIGTERM first, then SIGKILL if processes don't exit gracefully.
        This ensures child processes spawned by the shell command are also killed.

        Args:
            proc: The Popen process object
        """
        try:
            # Get the process group ID (same as PID since we used start_new_session=True)
            pgid = os.getpgid(proc.pid)

            # Send SIGTERM to the process group
            os.killpg(pgid, signal.SIGTERM)

            # Give processes time to exit gracefully
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill with SIGKILL if still running
                try:
                    os.killpg(pgid, signal.SIGKILL)
                    proc.wait(timeout=2)
                except (ProcessLookupError, OSError):
                    pass  # Process already dead

        except (ProcessLookupError, OSError):
            # Process or process group already dead
            pass
