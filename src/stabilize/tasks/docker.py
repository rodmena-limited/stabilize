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
