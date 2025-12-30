from __future__ import annotations
import logging
import subprocess
from typing import TYPE_CHECKING, Any
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
logger = logging.getLogger(__name__)

class DockerTask(Task):
    """
    Execute Docker commands.

    Supports container lifecycle, image management, and inspection commands.

    Context Parameters:
        action (str): Action to perform (default: run)
            - run: Run a container
            - exec: Execute command in running container
            - build: Build an image
            - pull: Pull an image
            - ps: List containers
            - images: List images
            - logs: Get container logs
            - stop: Stop a container
            - rm: Remove a container

        For 'run' action:
            image (str): Docker image name (required)
            command (str|list): Command to run in container (optional)
            name (str): Container name (optional)
            volumes (list[str]): Volume mounts as "host:container" (optional)
            ports (list[str]): Port mappings as "host:container" (optional)
            environment (dict): Environment variables (optional)
            workdir (str): Working directory in container (optional)
            network (str): Docker network to connect (optional)
            remove (bool): Remove container after run (default: True)
            detach (bool): Run in detached mode (default: False)

        For 'exec' action:
            name (str): Container name (required)
            command (str|list): Command to execute (required)

        For 'build' action:
            tag (str): Image tag (optional)
            dockerfile (str): Dockerfile path (default: Dockerfile)
            context (str): Build context path (default: .)
            build_args (dict): Build arguments (optional)
            no_cache (bool): Disable build cache (default: False)

        Common:
            timeout (int): Command timeout in seconds (default: 300)
            continue_on_failure (bool): Return failed_continue on error

    Outputs:
        stdout (str): Command standard output
        stderr (str): Command standard error
        exit_code (int): Command exit code
        container_id (str): Container ID (for run with detach)
        image_id (str): Image ID (for build)

    Examples:
        # Run container
        context = {
            "action": "run",
            "image": "alpine:latest",
            "command": "echo Hello",
        }

        # Run with environment and volumes
        context = {
            "action": "run",
            "image": "python:3.11",
            "volumes": ["/app:/app"],
            "environment": {"DEBUG": "true"},
            "command": "python /app/script.py",
        }

        # Build image
        context = {
            "action": "build",
            "tag": "myapp:latest",
            "context": "./docker",
        }
    """
    SUPPORTED_ACTIONS = frozenset({'run', 'exec', 'build', 'pull', 'ps', 'images', 'logs', 'stop', 'rm'})

    def execute(self, stage: StageExecution) -> TaskResult:
        """Execute Docker command."""
        action = stage.context.get("action", "run")
        timeout = stage.context.get("timeout", 300)
        continue_on_failure = stage.context.get("continue_on_failure", False)

        if action not in self.SUPPORTED_ACTIONS:
            return TaskResult.terminal(
                error=f"Unsupported action '{action}'. Supported: {sorted(self.SUPPORTED_ACTIONS)}"
            )

        # Check Docker availability
        try:
            subprocess.run(
                ["docker", "version"],
                capture_output=True,
                timeout=10,
                check=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
            return TaskResult.terminal(error="Docker is not available. Ensure Docker is installed and running.")

        # Build command based on action
        try:
            cmd = self._build_command(action, stage.context)
        except ValueError as e:
            return TaskResult.terminal(error=str(e))

        logger.debug(f"DockerTask executing: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            outputs: dict[str, Any] = {
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
                "exit_code": result.returncode,
            }

            # Extract container/image ID for relevant actions
            if action == "run" and stage.context.get("detach"):
                outputs["container_id"] = result.stdout.strip()[:12]
            elif action == "build" and result.returncode == 0:
                for line in result.stdout.split("\n"):
                    if "Successfully built" in line:
                        outputs["image_id"] = line.split()[-1]
                        break

            if result.returncode == 0:
                logger.debug("DockerTask success")
                return TaskResult.success(outputs=outputs)
            else:
                error_msg = f"Docker command failed with exit code {result.returncode}"
                if continue_on_failure:
                    return TaskResult.failed_continue(error=error_msg, outputs=outputs)
                return TaskResult.terminal(error=error_msg, context=outputs)

        except subprocess.TimeoutExpired:
            error_msg = f"Docker command timed out after {timeout}s"
            if continue_on_failure:
                return TaskResult.failed_continue(error=error_msg)
            return TaskResult.terminal(error=error_msg)

    def _build_command(self, action: str, context: dict[str, Any]) -> list[str]:
        """Build Docker command based on action and context."""
        if action == "run":
            return self._build_run_command(context)
        elif action == "exec":
            return self._build_exec_command(context)
        elif action == "build":
            return self._build_build_command(context)
        elif action == "pull":
            image = context.get("image")
            if not image:
                raise ValueError("'image' is required for pull action")
            return ["docker", "pull", image]
        elif action == "ps":
            cmd = ["docker", "ps"]
            if context.get("all"):
                cmd.append("-a")
            return cmd
        elif action == "images":
            return ["docker", "images"]
        elif action == "logs":
            name = context.get("name")
            if not name:
                raise ValueError("'name' is required for logs action")
            cmd = ["docker", "logs"]
            if context.get("follow"):
                cmd.append("-f")
            if context.get("tail"):
                cmd.extend(["--tail", str(context["tail"])])
            cmd.append(name)
            return cmd
        elif action == "stop":
            name = context.get("name")
            if not name:
                raise ValueError("'name' is required for stop action")
            return ["docker", "stop", name]
        elif action == "rm":
            name = context.get("name")
            if not name:
                raise ValueError("'name' is required for rm action")
            cmd = ["docker", "rm"]
            if context.get("force"):
                cmd.append("-f")
            cmd.append(name)
            return cmd
        else:
            raise ValueError(f"Unknown action: {action}")

    def _build_run_command(self, context: dict[str, Any]) -> list[str]:
        """Build docker run command."""
        image = context.get("image")
        if not image:
            raise ValueError("'image' is required for run action")

        cmd = ["docker", "run"]

        # Container name
        if context.get("name"):
            cmd.extend(["--name", context["name"]])

        # Remove after exit
        if context.get("remove", True):
            cmd.append("--rm")

        # Detach mode
        if context.get("detach"):
            cmd.append("-d")

        # Volumes
        for vol in context.get("volumes", []):
            cmd.extend(["-v", vol])

        # Ports
        for port in context.get("ports", []):
            cmd.extend(["-p", port])

        # Environment variables
        for key, value in context.get("environment", {}).items():
            cmd.extend(["-e", f"{key}={value}"])

        # Working directory
        if context.get("workdir"):
            cmd.extend(["-w", context["workdir"]])

        # Network
        if context.get("network"):
            cmd.extend(["--network", context["network"]])

        # Image
        cmd.append(image)

        # Command
        container_cmd = context.get("command")
        if container_cmd:
            if isinstance(container_cmd, str):
                cmd.extend(container_cmd.split())
            else:
                cmd.extend(container_cmd)

        return cmd
