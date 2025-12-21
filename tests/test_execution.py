from typing import Any
from stabilize import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    Orchestrator,
    QueueProcessor,
    RunTaskHandler,
    StageExecution,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.persistence.memory import InMemoryWorkflowStore
from stabilize.queue.queue import InMemoryQueue

def setup_stabilize() -> tuple[
    InMemoryQueue,
    InMemoryWorkflowStore,
    QueueProcessor,
    Orchestrator,
]:
    """Set up a complete pipeline runner for testing."""
    queue = InMemoryQueue()
    repository = InMemoryWorkflowStore()
    task_registry = TaskRegistry()

    # Register test tasks
    task_registry.register("success", SuccessTask)
    task_registry.register("fail", FailTask)
    task_registry.register("counter", CounterTask)

    # Create processor
    processor = QueueProcessor(queue)

    handlers: list[Any] = [
        StartWorkflowHandler(queue, repository),
        StartStageHandler(queue, repository),
        StartTaskHandler(queue, repository),
        RunTaskHandler(queue, repository, task_registry),
        CompleteTaskHandler(queue, repository),
        CompleteStageHandler(queue, repository),
        CompleteWorkflowHandler(queue, repository),
    ]

    for handler in handlers:
        processor.register_handler(handler)

    runner = Orchestrator(queue)

    return queue, repository, processor, runner

class SuccessTask(Task):
    """A task that always succeeds."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"success": True})

class FailTask(Task):
    """A task that always fails."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("Task failed intentionally")

class CounterTask(Task):
    """A task that increments a counter."""
    counter: int = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        CounterTask.counter += 1
        return TaskResult.success(outputs={"count": CounterTask.counter})
