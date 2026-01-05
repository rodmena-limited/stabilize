#!/usr/bin/env python3
"""
Docker Example - Demonstrates running Docker containers with Stabilize.

This example shows how to use the built-in DockerTask for:
1. Running containers and executing commands
2. Building images and managing container lifecycle
3. Building CI/CD-style container workflows

Requirements:
    Docker CLI installed and running
    User must have permissions to run docker commands

Run with:
    python examples/docker-example.py
"""

import logging
from typing import Any

logging.basicConfig(level=logging.ERROR)

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    DockerTask,
    Orchestrator,
    Queue,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStatus,
    WorkflowStore,
)

# =============================================================================
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
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


def example_simple_run() -> None:
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


def example_pull_and_run() -> None:
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


def example_with_environment() -> None:
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


def example_parallel_containers() -> None:
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


def example_list_containers() -> None:
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
