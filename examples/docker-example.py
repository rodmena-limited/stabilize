import logging
from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    DockerTask,
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
    WorkflowStatus,
)
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.queue import Queue

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
