"""Message handlers for pipeline execution."""

from typing import Any

from stabilize.handlers.base import MessageHandler, StabilizeHandler
from stabilize.handlers.complete_stage import CompleteStageHandler
from stabilize.handlers.complete_task import CompleteTaskHandler
from stabilize.handlers.complete_workflow import CompleteWorkflowHandler
from stabilize.handlers.run_task import RunTaskHandler
from stabilize.handlers.start_stage import StartStageHandler
from stabilize.handlers.start_task import StartTaskHandler
from stabilize.handlers.start_workflow import StartWorkflowHandler

__all__ = [
    "MessageHandler",
    "StabilizeHandler",
    "StartWorkflowHandler",
    "StartStageHandler",
    "StartTaskHandler",
    "RunTaskHandler",
    "CompleteTaskHandler",
    "CompleteStageHandler",
    "CompleteWorkflowHandler",
]


def register_all_handlers(
    processor: Any,
    repository: Any,
    task_registry: Any,
    queue: Any,
) -> None:
    """
    Register all handlers with a queue processor.

    Args:
        processor: The queue processor to register with
        repository: The execution repository
        task_registry: The task registry
        queue: The message queue
    """
    handlers = [
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
