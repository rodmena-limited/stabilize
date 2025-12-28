from __future__ import annotations
import base64
import logging
import os
import subprocess
from typing import TYPE_CHECKING, Any
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
logger = logging.getLogger(__name__)
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
