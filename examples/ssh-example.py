#!/usr/bin/env python3
"""
SSH Example - Demonstrates executing remote commands via SSH with Stabilize.

This example shows how to use the built-in SSHTask for:
1. Running commands on remote servers
2. Building deployment and administration workflows
3. Parallel health checks across multiple hosts

Requirements:
    SSH client installed (ssh command available)
    SSH access to target hosts (key-based auth recommended)

Run with:
    python examples/ssh-example.py
"""

import logging
from typing import Any

logging.basicConfig(level=logging.ERROR)

from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    Queue,
    QueueProcessor,
    RunTaskHandler,
    SqliteQueue,
    SqliteWorkflowStore,
    SSHTask,
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
    """Create processor and orchestrator with SSHTask registered."""
    task_registry = TaskRegistry()
    task_registry.register("ssh", SSHTask)

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
# Example 1: Simple Remote Command
# =============================================================================


def example_simple_command() -> None:
    """Execute a simple command on a remote host."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Remote Command")
    print("=" * 60)
    print("Note: Requires SSH access to localhost or modify host")

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Using localhost for demo - change to actual remote host
    workflow = Workflow.create(
        application="ssh-example",
        name="Simple Command",
        stages=[
            StageExecution(
                ref_id="1",
                type="ssh",
                name="Get Hostname",
                context={
                    "host": "localhost",
                    "command": "hostname && uname -a",
                    "timeout": 30,
                },
                tasks=[
                    TaskExecution.create(
                        name="SSH Command",
                        implementing_class="ssh",
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
    if result.status == WorkflowStatus.SUCCEEDED:
        print(f"Output: {result.stages[0].outputs.get('stdout')}")
    else:
        print(f"Error: {result.stages[0].outputs.get('stderr', 'Connection failed')}")


# =============================================================================
# Example 2: Sequential Deployment Steps
# =============================================================================


def example_sequential_deployment() -> None:
    """Sequential deployment: check -> deploy -> verify."""
    print("\n" + "=" * 60)
    print("Example 2: Sequential Deployment Steps")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    host = "localhost"

    workflow = Workflow.create(
        application="ssh-example",
        name="Deployment Pipeline",
        stages=[
            # Step 1: Pre-flight check
            StageExecution(
                ref_id="1",
                type="ssh",
                name="Pre-flight Check",
                context={
                    "host": host,
                    "command": "echo 'Checking system...' && df -h / | tail -1 && free -m | head -2",
                },
                tasks=[
                    TaskExecution.create(
                        name="Check System",
                        implementing_class="ssh",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Step 2: Create deployment directory
            StageExecution(
                ref_id="2",
                type="ssh",
                name="Prepare Directory",
                requisite_stage_ref_ids={"1"},
                context={
                    "host": host,
                    "command": "mkdir -p /tmp/stabilize_deploy && echo 'Directory ready'",
                },
                tasks=[
                    TaskExecution.create(
                        name="Prepare",
                        implementing_class="ssh",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Step 3: Deploy (simulate)
            StageExecution(
                ref_id="3",
                type="ssh",
                name="Deploy Application",
                requisite_stage_ref_ids={"2"},
                context={
                    "host": host,
                    "command": "echo 'Deploying...' && echo 'version=0.9.0' > /tmp/stabilize_deploy/app.conf && cat /tmp/stabilize_deploy/app.conf",
                },
                tasks=[
                    TaskExecution.create(
                        name="Deploy",
                        implementing_class="ssh",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Step 4: Verify
            StageExecution(
                ref_id="4",
                type="ssh",
                name="Verify Deployment",
                requisite_stage_ref_ids={"3"},
                context={
                    "host": host,
                    "command": "test -f /tmp/stabilize_deploy/app.conf && echo 'Deployment verified' || echo 'Deployment failed'",
                },
                tasks=[
                    TaskExecution.create(
                        name="Verify",
                        implementing_class="ssh",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            # Step 5: Cleanup
            StageExecution(
                ref_id="5",
                type="ssh",
                name="Cleanup",
                requisite_stage_ref_ids={"4"},
                context={
                    "host": host,
                    "command": "rm -rf /tmp/stabilize_deploy && echo 'Cleanup complete'",
                },
                tasks=[
                    TaskExecution.create(
                        name="Cleanup",
                        implementing_class="ssh",
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
        first_line = stdout.split("\n")[0][:50] if stdout else "N/A"
        print(f"  {status_mark} {stage.name}: {first_line}")


# =============================================================================
# Example 3: Parallel Health Checks
# =============================================================================


def example_parallel_health_check() -> None:
    """Check multiple servers in parallel."""
    print("\n" + "=" * 60)
    print("Example 3: Parallel Health Checks")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Using localhost multiple times to simulate multiple servers
    # In production, these would be different hosts
    hosts = [
        ("server1", "localhost"),
        ("server2", "localhost"),
        ("server3", "localhost"),
    ]

    #      Start
    #     /  |  \
    # S1   S2   S3
    #     \  |  /
    #     Report

    stages = [
        StageExecution(
            ref_id="start",
            type="ssh",
            name="Start Health Check",
            context={
                "host": "localhost",
                "command": "echo 'Starting health checks...'",
            },
            tasks=[
                TaskExecution.create(
                    name="Start",
                    implementing_class="ssh",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
        ),
    ]

    # Parallel health checks
    for name, host in hosts:
        stages.append(
            StageExecution(
                ref_id=name,
                type="ssh",
                name=f"Check {name}",
                requisite_stage_ref_ids={"start"},
                context={
                    "host": host,
                    "command": f"echo 'Checking {name}...' && uptime && echo 'Status: OK'",
                    "continue_on_failure": True,  # Continue even if one fails
                },
                tasks=[
                    TaskExecution.create(
                        name=f"Check {name}",
                        implementing_class="ssh",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            )
        )

    # Report stage
    stages.append(
        StageExecution(
            ref_id="report",
            type="ssh",
            name="Generate Report",
            requisite_stage_ref_ids={name for name, _ in hosts},
            context={
                "host": "localhost",
                "command": "echo 'Health check complete' && date",
            },
            tasks=[
                TaskExecution.create(
                    name="Report",
                    implementing_class="ssh",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
        )
    )

    workflow = Workflow.create(
        application="ssh-example",
        name="Parallel Health Check",
        stages=stages,
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=120.0)

    result = store.retrieve(workflow.id)
    print(f"\nWorkflow Status: {result.status}")
    for stage in result.stages:
        status_mark = "[OK]" if stage.status == WorkflowStatus.SUCCEEDED else "[FAIL]"
        host = stage.outputs.get("host", "N/A")
        stdout = stage.outputs.get("stdout", "")
        status_line = [line for line in stdout.split("\n") if "Status:" in line or "complete" in line.lower()]
        status = status_line[0] if status_line else "N/A"
        print(f"  {status_mark} {stage.name} ({host}): {status[:40]}")


# =============================================================================
# Example 4: Multi-Command Script
# =============================================================================


def example_multi_command() -> None:
    """Execute a multi-command script on remote host."""
    print("\n" + "=" * 60)
    print("Example 4: Multi-Command Script")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    # Multi-line script as single command
    script = """
echo '=== System Information ==='
echo "Hostname: $(hostname)"
echo "Kernel: $(uname -r)"
echo "Uptime: $(uptime -p)"
echo ''
echo '=== Disk Usage ==='
df -h / | tail -1
echo ''
echo '=== Memory Usage ==='
free -h | head -2
echo ''
echo '=== Load Average ==='
cat /proc/loadavg
echo ''
echo '=== Done ==='
"""

    workflow = Workflow.create(
        application="ssh-example",
        name="System Report",
        stages=[
            StageExecution(
                ref_id="1",
                type="ssh",
                name="Generate System Report",
                context={
                    "host": "localhost",
                    "command": script.replace("\n", " && ").strip(" && "),
                    "timeout": 60,
                },
                tasks=[
                    TaskExecution.create(
                        name="System Report",
                        implementing_class="ssh",
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
    if result.status == WorkflowStatus.SUCCEEDED:
        print("\nSystem Report:")
        print("-" * 40)
        print(result.stages[0].outputs.get("stdout", ""))


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("Stabilize SSH Examples")
    print("=" * 60)
    print("Requires: SSH client and access to target hosts")
    print("Note: Examples use localhost - modify hosts for real usage")

    example_simple_command()
    example_sequential_deployment()
    example_parallel_health_check()
    example_multi_command()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
