"""
SSHTask for executing commands on remote hosts via SSH.

This module provides a production-ready SSHTask with:
- SSH command execution via subprocess
- Key-based and password-less authentication
- Connection timeout handling
- Host key checking options
- Continue on failure support
"""

from __future__ import annotations

import logging
import os
import subprocess
from typing import TYPE_CHECKING, Any

from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)


class SSHTask(Task):
    """
    Execute commands on remote hosts via SSH.

    Uses the ssh CLI command via subprocess for maximum compatibility.
    Key-based authentication is recommended for automation.

    Context Parameters:
        host (str): Remote hostname or IP address (required)
        user (str): SSH username (default: current user)
        command (str): Command to execute on remote host (required)
        port (int): SSH port (default: 22)
        key_file (str): Path to private key file (optional)
        timeout (int): Command timeout in seconds (default: 60)
        strict_host_key (bool): Strict host key checking (default: False)
        connect_timeout (int): SSH connection timeout (default: 10)
        continue_on_failure (bool): Return failed_continue instead of terminal on error

    Outputs:
        stdout (str): Command standard output
        stderr (str): Command standard error
        exit_code (int): Remote command exit code
        host (str): Target host
        user (str): SSH user

    Example:
        # Simple command
        context = {"host": "server.example.com", "command": "uptime"}

        # With key file
        context = {
            "host": "server.example.com",
            "user": "deploy",
            "key_file": "/home/user/.ssh/deploy_key",
            "command": "systemctl status nginx",
        }

        # Continue on failure (for health checks)
        context = {
            "host": "server.example.com",
            "command": "test -f /app/healthcheck",
            "continue_on_failure": True,
        }
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        """Execute SSH command on remote host."""
        host = stage.context.get("host")
        user = stage.context.get("user", os.environ.get("USER", "root"))
        command = stage.context.get("command")
        port = stage.context.get("port", 22)
        key_file = stage.context.get("key_file")
        timeout = stage.context.get("timeout", 60)
        strict_host_key = stage.context.get("strict_host_key", False)
        connect_timeout = stage.context.get("connect_timeout", 10)
        continue_on_failure = stage.context.get("continue_on_failure", False)

        if not host:
            return TaskResult.terminal(error="No 'host' specified in context")

        if not command:
            return TaskResult.terminal(error="No 'command' specified in context")

        # Check SSH availability
        try:
            subprocess.run(
                ["ssh", "-V"],
                capture_output=True,
                timeout=5,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return TaskResult.terminal(error="SSH client not available. Ensure ssh is installed.")

        # Build SSH command
        ssh_cmd = ["ssh"]

        # Port
        if port != 22:
            ssh_cmd.extend(["-p", str(port)])

        # Key file
        if key_file:
            ssh_cmd.extend(["-i", key_file])

        # Connection timeout
        ssh_cmd.extend(["-o", f"ConnectTimeout={connect_timeout}"])

        # Host key checking
        if not strict_host_key:
            ssh_cmd.extend(["-o", "StrictHostKeyChecking=no"])
            ssh_cmd.extend(["-o", "UserKnownHostsFile=/dev/null"])

        # Disable pseudo-terminal allocation for non-interactive commands
        ssh_cmd.append("-T")

        # Batch mode (no password prompts)
        ssh_cmd.extend(["-o", "BatchMode=yes"])

        # Target
        target = f"{user}@{host}"
        ssh_cmd.append(target)

        # Command
        ssh_cmd.append(command)

        logger.debug(f"SSHTask executing: {user}@{host}: {command}")

        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            outputs: dict[str, Any] = {
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
                "exit_code": result.returncode,
                "host": host,
                "user": user,
            }

            if result.returncode == 0:
                logger.debug(f"SSHTask success on {host}")
                return TaskResult.success(outputs=outputs)
            elif result.returncode == 255:
                # SSH connection error
                error_msg = f"SSH connection failed to {host}: {result.stderr}"
                if continue_on_failure:
                    return TaskResult.failed_continue(error=error_msg, outputs=outputs)
                return TaskResult.terminal(error=error_msg, context=outputs)
            else:
                error_msg = f"Remote command failed with exit code {result.returncode}"
                if continue_on_failure:
                    return TaskResult.failed_continue(error=error_msg, outputs=outputs)
                return TaskResult.terminal(error=error_msg, context=outputs)

        except subprocess.TimeoutExpired:
            error_msg = f"SSH command timed out after {timeout}s"
            outputs = {"host": host, "user": user}
            if continue_on_failure:
                return TaskResult.failed_continue(error=error_msg, outputs=outputs)
            return TaskResult.terminal(error=error_msg, context=outputs)
