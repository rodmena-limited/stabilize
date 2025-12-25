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
