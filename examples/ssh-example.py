import logging
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
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
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

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
