#!/usr/bin/env python3
"""
Docker Example - Demonstrates running Docker containers with Stabilize.

This example shows how to:
1. Create a custom Task that runs Docker commands
2. Run containers, build images, and manage docker-compose
3. Build CI/CD-style container workflows

Requirements:
    Docker CLI installed and running
    User must have permissions to run docker commands

Run with:
    python examples/docker-example.py
"""

import json
import logging
import subprocess
from typing import Any

logging.basicConfig(level=logging.ERROR)

from stabilize import Workflow, StageExecution, TaskExecution, WorkflowStatus
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.sqlite_queue import SqliteQueue
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.queue import Queue
from stabilize.persistence.store import WorkflowStore
from stabilize.orchestrator import Orchestrator
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from stabilize.tasks.registry import TaskRegistry
from stabilize.handlers.complete_workflow import CompleteWorkflowHandler
from stabilize.handlers.complete_stage import CompleteStageHandler
from stabilize.handlers.complete_task import CompleteTaskHandler
from stabilize.handlers.run_task import RunTaskHandler
from stabilize.handlers.start_workflow import StartWorkflowHandler
from stabilize.handlers.start_stage import StartStageHandler
from stabilize.handlers.start_task import StartTaskHandler


# =============================================================================
# Custom Task: DockerTask
# =============================================================================


class DockerTask(Task):
    """
    Execute Docker commands.

    Context Parameters:
        action: Action to perform - run, exec, build, pull, ps, images, logs, stop, rm
        image: Docker image name (for run, pull, build)
        command: Command to run in container (optional)
        name: Container name (optional)
        tag: Image tag for build (optional)
        dockerfile: Dockerfile path for build (default: Dockerfile)
        context: Build context path (default: .)
        volumes: Volume mounts as list of "host:container" strings (optional)
        ports: Port mappings as list of "host:container" strings (optional)
        environment: Environment variables as dict (optional)
        workdir: Working directory in container (optional)
        network: Docker network to connect (optional)
        remove: Remove container after run (default: True)
        detach: Run in detached mode (default: False)
        timeout: Command timeout in seconds (default: 300)

    Outputs:
        stdout: Command standard output
        stderr: Command standard error
        exit_code: Command exit code
        container_id: Container ID (for run with detach)
        image_id: Image ID (for build)
    """

    SUPPORTED_ACTIONS = {"run", "exec", "build", "pull", "ps", "images", "logs", "stop", "rm"}

    def execute(self, stage: StageExecution) -> TaskResult:
        action = stage.context.get("action", "run")
        timeout = stage.context.get("timeout", 300)

        if action not in self.SUPPORTED_ACTIONS:
            return TaskResult.terminal(
                error=f"Unsupported action '{action}'. Supported: {self.SUPPORTED_ACTIONS}"
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
            return TaskResult.terminal(
                error="Docker is not available. Ensure Docker is installed and running."
            )

        # Build command based on action
        try:
            cmd = self._build_command(action, stage.context)
        except ValueError as e:
            return TaskResult.terminal(error=str(e))

        print(f"  [DockerTask] {' '.join(cmd)}")

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
                # Try to get image ID from output
                for line in result.stdout.split("\n"):
                    if "Successfully built" in line:
                        outputs["image_id"] = line.split()[-1]
                        break

            if result.returncode == 0:
                print(f"  [DockerTask] Success")
                return TaskResult.success(outputs=outputs)
            else:
                print(f"  [DockerTask] Failed with exit code {result.returncode}")
                if stage.context.get("continue_on_failure"):
                    return TaskResult.failed_continue(
                        error=f"Docker command failed with exit code {result.returncode}",
                        outputs=outputs,
                    )
                return TaskResult.terminal(
                    error=f"Docker command failed with exit code {result.returncode}",
                    context=outputs,
                )

        except subprocess.TimeoutExpired:
            return TaskResult.terminal(error=f"Docker command timed out after {timeout}s")

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

    def _build_exec_command(self, context: dict[str, Any]) -> list[str]:
        """Build docker exec command."""
        name = context.get("name")
        command = context.get("command")

        if not name:
            raise ValueError("'name' is required for exec action")
        if not command:
            raise ValueError("'command' is required for exec action")

        cmd = ["docker", "exec"]

        # Interactive/TTY
        if context.get("interactive"):
            cmd.append("-i")
        if context.get("tty"):
            cmd.append("-t")

        # Working directory
        if context.get("workdir"):
            cmd.extend(["-w", context["workdir"]])

        # Environment variables
        for key, value in context.get("environment", {}).items():
            cmd.extend(["-e", f"{key}={value}"])

        cmd.append(name)

        if isinstance(command, str):
            cmd.extend(command.split())
        else:
            cmd.extend(command)

        return cmd

    def _build_build_command(self, context: dict[str, Any]) -> list[str]:
        """Build docker build command."""
        cmd = ["docker", "build"]

        # Tag
        tag = context.get("tag") or context.get("image")
        if tag:
            cmd.extend(["-t", tag])

        # Dockerfile
        if context.get("dockerfile"):
            cmd.extend(["-f", context["dockerfile"]])

        # Build args
        for key, value in context.get("build_args", {}).items():
            cmd.extend(["--build-arg", f"{key}={value}"])

        # No cache
        if context.get("no_cache"):
            cmd.append("--no-cache")

        # Context path
        build_context = context.get("context", ".")
        cmd.append(build_context)

        return cmd


# =============================================================================
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(
    store: WorkflowStore, queue: Queue
) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with DockerTask registered."""
    task_registry = TaskRegistry()
    task_registry.register("docker", DockerTask)

    processor = QueueProcessor(queue)

    handlers: list[Any] = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, task_registry),
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]

    for handler in handlers:
        processor.register_handler(handler)

    orchestrator = Orchestrator(queue)
    return processor, orchestrator


# =============================================================================
# Example 1: Simple Container Run
# =============================================================================


def example_simple_run():
    """Run a simple container command."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Container Run")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="docker-example",
        name="Simple Run",
        stages=[
            StageExecution(
                ref_id="1",
                type="docker",
                name="Run Alpine",
                context={
                    "action": "run",
                    "image": "alpine:latest",
                    "command": "echo Hello from Docker",
                },
                tasks=[
                    TaskExecution.create(
                        name="Docker Run",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print(f"Output: {result.stages[0].outputs.get('stdout')}")


# =============================================================================
# Example 2: Pull and Run
# =============================================================================


def example_pull_and_run():
    """Pull an image then run a container."""
    print("\n" + "=" * 60)
    print("Example 2: Pull and Run")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="docker-example",
        name="Pull and Run",
        stages=[
            StageExecution(
                ref_id="1",
                type="docker",
                name="Pull Image",
                context={
                    "action": "pull",
                    "image": "busybox:latest",
                },
                tasks=[
                    TaskExecution.create(
                        name="Docker Pull",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="2",
                type="docker",
                name="Run Container",
                requisite_stage_ref_ids={"1"},
                context={
                    "action": "run",
                    "image": "busybox:latest",
                    "command": "uname -a",
                },
                tasks=[
                    TaskExecution.create(
                        name="Docker Run",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=120.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    for stage in result.stages:
        stdout = stage.outputs.get("stdout", "")
        print(f"  {stage.name}: {stdout[:100]}")


# =============================================================================
# Example 3: Container with Environment Variables
# =============================================================================


def example_with_environment():
    """Run container with environment variables and volumes."""
    print("\n" + "=" * 60)
    print("Example 3: Container with Environment")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="docker-example",
        name="Environment Variables",
        stages=[
            StageExecution(
                ref_id="1",
                type="docker",
                name="Run with Env",
                context={
                    "action": "run",
                    "image": "alpine:latest",
                    "environment": {
                        "APP_NAME": "Stabilize",
                        "APP_VERSION": "0.9.0",
                        "DEBUG": "true",
                    },
                    "command": "sh -c 'echo App: $APP_NAME v$APP_VERSION, Debug: $DEBUG'",
                },
                tasks=[
                    TaskExecution.create(
                        name="Docker Run",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print(f"Output: {result.stages[0].outputs.get('stdout')}")


# =============================================================================
# Example 4: Parallel Container Operations
# =============================================================================


def example_parallel_containers():
    """Run multiple containers in parallel."""
    print("\n" + "=" * 60)
    print("Example 4: Parallel Containers")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    #       Start
    #      /  |  \
    # Task1 Task2 Task3
    #      \  |  /
    #       Done

    workflow = Workflow.create(
        application="docker-example",
        name="Parallel Containers",
        stages=[
            StageExecution(
                ref_id="start",
                type="docker",
                name="Start",
                context={
                    "action": "run",
                    "image": "alpine:latest",
                    "command": "echo Starting parallel tasks",
                },
                tasks=[
                    TaskExecution.create(
                        name="Start",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Parallel tasks
            StageExecution(
                ref_id="task1",
                type="docker",
                name="Task 1: CPU Info",
                requisite_stage_ref_ids={"start"},
                context={
                    "action": "run",
                    "image": "alpine:latest",
                    "command": "cat /proc/cpuinfo | head -10",
                },
                tasks=[
                    TaskExecution.create(
                        name="CPU Info",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="task2",
                type="docker",
                name="Task 2: Memory Info",
                requisite_stage_ref_ids={"start"},
                context={
                    "action": "run",
                    "image": "alpine:latest",
                    "command": "cat /proc/meminfo | head -5",
                },
                tasks=[
                    TaskExecution.create(
                        name="Memory Info",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="task3",
                type="docker",
                name="Task 3: Disk Info",
                requisite_stage_ref_ids={"start"},
                context={
                    "action": "run",
                    "image": "alpine:latest",
                    "command": "df -h",
                },
                tasks=[
                    TaskExecution.create(
                        name="Disk Info",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Join
            StageExecution(
                ref_id="done",
                type="docker",
                name="Done",
                requisite_stage_ref_ids={"task1", "task2", "task3"},
                context={
                    "action": "run",
                    "image": "alpine:latest",
                    "command": "echo All parallel tasks completed",
                },
                tasks=[
                    TaskExecution.create(
                        name="Done",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=120.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    for stage in result.stages:
        status_mark = "[OK]" if stage.status == WorkflowStatus.SUCCEEDED else "[FAIL]"
        stdout = stage.outputs.get("stdout", "")
        first_line = stdout.split("\n")[0][:50] if stdout else ""
        print(f"  {status_mark} {stage.name}: {first_line}")


# =============================================================================
# Example 5: List Running Containers
# =============================================================================


def example_list_containers():
    """List Docker containers and images."""
    print("\n" + "=" * 60)
    print("Example 5: List Containers and Images")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="docker-example",
        name="List Resources",
        stages=[
            StageExecution(
                ref_id="1",
                type="docker",
                name="List Containers",
                context={
                    "action": "ps",
                    "all": True,
                },
                tasks=[
                    TaskExecution.create(
                        name="Docker PS",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="2",
                type="docker",
                name="List Images",
                requisite_stage_ref_ids={"1"},
                context={
                    "action": "images",
                },
                tasks=[
                    TaskExecution.create(
                        name="Docker Images",
                        implementing_class="docker",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")

    for stage in result.stages:
        print(f"\n{stage.name}:")
        stdout = stage.outputs.get("stdout", "")
        # Show first few lines
        lines = stdout.split("\n")[:5]
        for line in lines:
            print(f"  {line[:80]}")
        if len(stdout.split("\n")) > 5:
            print(f"  ... ({len(stdout.split(chr(10))) - 5} more lines)")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("Stabilize Docker Examples")
    print("=" * 60)
    print("Requires: Docker installed and running")

    example_simple_run()
    example_pull_and_run()
    example_with_environment()
    example_parallel_containers()
    example_list_containers()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
