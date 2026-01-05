#!/usr/bin/env python3
"""
Shell Command Example - Demonstrates running shell commands with Stabilize.

This example shows how to use the built-in ShellTask for:
1. Single commands
2. Sequential pipelines with output passing
3. Parallel command execution

Run with:
    python examples/shell-example.py
"""

import logging
from typing import Any

# Configure logging before importing stabilize modules
logging.basicConfig(level=logging.ERROR)  # Suppress all but errors

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    Queue,
    QueueProcessor,
    RunTaskHandler,
    ShellTask,
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
    """Create processor and orchestrator with ShellTask registered."""
    # Create task registry and register our ShellTask
    task_registry = TaskRegistry()
    task_registry.register("shell", ShellTask)

    # Create message processor
    processor = QueueProcessor(queue)

    # Register all handlers (this is how the engine processes workflow messages)
    handlers: list[Any] = [
        StartWorkflowHandler(queue, store),
        StartStageHandler(queue, store),
        StartTaskHandler(queue, store),
        RunTaskHandler(queue, store, task_registry),  # This executes our ShellTask
        CompleteTaskHandler(queue, store),
        CompleteStageHandler(queue, store),
        CompleteWorkflowHandler(queue, store),
    ]

    for handler in handlers:
        processor.register_handler(handler)

    # Create orchestrator (starts workflows)
    orchestrator = Orchestrator(queue)

    return processor, orchestrator


# =============================================================================
# Example 1: Simple Single Command
# =============================================================================


def example_simple() -> None:
    """Run a single shell command."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Single Command")
    print("=" * 60)

    # Setup - use in-memory SQLite for simplicity
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Create workflow
    workflow = Workflow.create(
        application="shell-example",
        name="Simple Command",
        stages=[
            StageExecution(
                ref_id="1",
                type="shell",
                name="List Current Directory",
                context={"command": "ls -la"},
                tasks=[
                    TaskExecution.create(
                        name="Run ls",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    # Run
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Check result
    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    print("Stage Output (first 200 chars):")
    stdout = result.stages[0].outputs.get("stdout", "")
    print(stdout[:200] + "..." if len(stdout) > 200 else stdout)


# =============================================================================
# Example 2: Sequential Commands (Pipeline)
# =============================================================================


def example_sequential() -> None:
    """Run multiple commands in sequence."""
    print("\n" + "=" * 60)
    print("Example 2: Sequential Commands")
    print("=" * 60)

    # Setup
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Create workflow: create dir -> create file -> read file
    workflow = Workflow.create(
        application="shell-example",
        name="Sequential Commands",
        stages=[
            StageExecution(
                ref_id="1",
                type="shell",
                name="Create Temp Directory",
                context={"command": "mkdir -p /tmp/stabilize_test"},
                tasks=[
                    TaskExecution.create(
                        name="mkdir",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="2",
                type="shell",
                name="Create File",
                requisite_stage_ref_ids={"1"},  # depends on stage 1
                context={"command": "echo 'Hello from Stabilize!' > /tmp/stabilize_test/hello.txt"},
                tasks=[
                    TaskExecution.create(
                        name="create file",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="3",
                type="shell",
                name="Read File",
                requisite_stage_ref_ids={"2"},  # depends on stage 2
                context={"command": "cat /tmp/stabilize_test/hello.txt"},
                tasks=[
                    TaskExecution.create(
                        name="read file",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="4",
                type="shell",
                name="Cleanup",
                requisite_stage_ref_ids={"3"},
                context={"command": "rm -rf /tmp/stabilize_test"},
                tasks=[
                    TaskExecution.create(
                        name="cleanup",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    # Run
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Check result
    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    for stage in result.stages:
        status_icon = "✓" if stage.status == WorkflowStatus.SUCCEEDED else "✗"
        print(f"  {status_icon} {stage.name}: {stage.status}")
        if stage.outputs.get("stdout"):
            print(f"      Output: {stage.outputs['stdout']}")


# =============================================================================
# Example 3: Parallel Commands
# =============================================================================


def example_parallel() -> None:
    """Run commands in parallel branches."""
    print("\n" + "=" * 60)
    print("Example 3: Parallel Commands")
    print("=" * 60)

    # Setup
    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Create workflow with parallel branches:
    #      Setup
    #     /     \
    #  Check1  Check2
    #     \     /
    #     Report
    workflow = Workflow.create(
        application="shell-example",
        name="Parallel Commands",
        stages=[
            StageExecution(
                ref_id="setup",
                type="shell",
                name="Setup",
                context={"command": "echo 'Starting parallel checks...'"},
                tasks=[
                    TaskExecution.create(
                        name="setup",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # These two run in parallel (both depend only on setup)
            StageExecution(
                ref_id="check1",
                type="shell",
                name="Check Python Version",
                requisite_stage_ref_ids={"setup"},
                context={"command": "python3 --version"},
                tasks=[
                    TaskExecution.create(
                        name="check python",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="check2",
                type="shell",
                name="Check Git Version",
                requisite_stage_ref_ids={"setup"},
                context={"command": "git --version"},
                tasks=[
                    TaskExecution.create(
                        name="check git",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # This waits for both parallel branches
            StageExecution(
                ref_id="report",
                type="shell",
                name="Report",
                requisite_stage_ref_ids={"check1", "check2"},
                context={"command": "echo 'All checks completed!'"},
                tasks=[
                    TaskExecution.create(
                        name="report",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    # Run
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Check result
    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    for stage in result.stages:
        status_icon = "✓" if stage.status == WorkflowStatus.SUCCEEDED else "✗"
        stdout = stage.outputs.get("stdout", "")
        print(f"  {status_icon} {stage.name}: {stdout}")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("Stabilize Shell Command Examples")
    print("=" * 60)

    example_simple()
    example_sequential()
    example_parallel()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
