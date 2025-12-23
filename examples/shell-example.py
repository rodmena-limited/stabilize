import logging
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
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
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

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
