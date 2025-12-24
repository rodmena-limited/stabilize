import json
import logging
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    HTTPTask,
    Orchestrator,
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
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

def setup_pipeline_runner(store: WorkflowStore, queue: Queue) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with HTTPTask registered."""
    task_registry = TaskRegistry()
    task_registry.register("http", HTTPTask)

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

def example_simple_get() -> None:
    """Make a simple GET request to a public API."""
    print("\n" + "=" * 60)
    print("Example 1: Simple GET Request")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    processor, orchestrator = setup_pipeline_runner(store, queue)

    workflow = Workflow.create(
        application="http-example",
        name="Simple GET",
        stages=[
            StageExecution(
                ref_id="1",
                type="http",
                name="Get IP Info",
                context={
                    "url": "https://httpbin.org/ip",
                    "method": "GET",
                },
                tasks=[
                    TaskExecution.create(
                        name="HTTP GET",
                        implementing_class="http",
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
    print(f"Response Status: {result.stages[0].outputs.get('status_code')}")
    print(f"Response Body: {result.stages[0].outputs.get('body', '')[:200]}")
