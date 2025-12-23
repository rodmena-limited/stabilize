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
