#!/usr/bin/env python3
"""
SSH Example - Demonstrates executing remote commands via SSH with Stabilize.

This example shows how to:
1. Create a custom Task that executes commands over SSH
2. Run commands on remote servers
3. Build deployment and administration workflows

Requirements:
    SSH client installed (ssh command available)
    SSH access to target hosts (key-based auth recommended)

Run with:
    python examples/ssh-example.py
"""

import logging
import os
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
# Custom Task: SSHTask
# =============================================================================


class SSHTask(Task):
    """
    Execute commands on remote hosts via SSH.

    Context Parameters:
        host: Remote hostname or IP address (required)
        user: SSH username (default: current user)
        command: Command to execute on remote host (required)
        port: SSH port (default: 22)
        key_file: Path to private key file (optional)
        timeout: Command timeout in seconds (default: 60)
        strict_host_key: Strict host key checking (default: False)
        connect_timeout: SSH connection timeout (default: 10)

    Outputs:
        stdout: Command standard output
        stderr: Command standard error
        exit_code: Remote command exit code
        host: Target host
        user: SSH user

    Notes:
        - Uses ssh CLI command via subprocess
        - Key-based authentication recommended
        - For password auth, use ssh-agent or sshpass (not recommended)
    """

    def execute(self, stage: StageExecution) -> TaskResult:
        host = stage.context.get("host")
        user = stage.context.get("user", os.environ.get("USER", "farshid"))
        command = stage.context.get("command")
        port = stage.context.get("port", 22)
        key_file = stage.context.get("key_file")
        timeout = stage.context.get("timeout", 60)
        strict_host_key = stage.context.get("strict_host_key", False)
        connect_timeout = stage.context.get("connect_timeout", 10)

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
            return TaskResult.terminal(
                error="SSH client not available. Ensure ssh is installed."
            )

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

        print(f"  [SSHTask] {user}@{host}: {command}")

        try:
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            outputs = {
                "stdout": result.stdout.strip(),
                "stderr": result.stderr.strip(),
                "exit_code": result.returncode,
                "host": host,
                "user": user,
            }

            if result.returncode == 0:
                print(f"  [SSHTask] Success on {host}")
                return TaskResult.success(outputs=outputs)
            elif result.returncode == 255:
                # SSH connection error
                print(f"  [SSHTask] Connection failed to {host}")
                return TaskResult.terminal(
                    error=f"SSH connection failed to {host}: {result.stderr}",
                    context=outputs,
                )
            else:
                print(f"  [SSHTask] Command failed on {host} with exit code {result.returncode}")
                if stage.context.get("continue_on_failure"):
                    return TaskResult.failed_continue(
                        error=f"Remote command failed with exit code {result.returncode}",
                        outputs=outputs,
                    )
                return TaskResult.terminal(
                    error=f"Remote command failed with exit code {result.returncode}",
                    context=outputs,
                )

        except subprocess.TimeoutExpired:
            return TaskResult.terminal(
                error=f"SSH command timed out after {timeout}s",
                context={"host": host, "user": user},
            )


# =============================================================================
# Helper: Setup pipeline infrastructure
# =============================================================================


def setup_pipeline_runner(
    store: WorkflowStore, queue: Queue
) -> tuple[QueueProcessor, Orchestrator]:
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


def example_simple_command():
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


def example_sequential_deployment():
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


def example_parallel_health_check():
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
        status_line = [l for l in stdout.split("\n") if "Status:" in l or "complete" in l.lower()]
        status = status_line[0] if status_line else "N/A"
        print(f"  {status_mark} {stage.name} ({host}): {status[:40]}")


# =============================================================================
# Example 4: Multi-Command Script
# =============================================================================


def example_multi_command():
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
