"""Message handlers for pipeline execution."""

from typing import Any

from stabilize.handlers.base import MessageHandler, StabilizeHandler
from stabilize.handlers.cancel_stage import CancelStageHandler
from stabilize.handlers.complete_stage import CompleteStageHandler
from stabilize.handlers.complete_task import CompleteTaskHandler
from stabilize.handlers.complete_workflow import CompleteWorkflowHandler
from stabilize.handlers.continue_parent_stage import ContinueParentStageHandler
from stabilize.handlers.jump_to_stage import JumpToStageHandler
from stabilize.handlers.run_task import RunTaskHandler
from stabilize.handlers.skip_stage import SkipStageHandler
from stabilize.handlers.start_stage import StartStageHandler
from stabilize.handlers.start_task import StartTaskHandler
from stabilize.handlers.start_waiting_workflows import StartWaitingWorkflowsHandler
from stabilize.handlers.start_workflow import StartWorkflowHandler

__all__ = [
    "MessageHandler",
    "StabilizeHandler",
    "StartWorkflowHandler",
    "StartStageHandler",
    "StartTaskHandler",
    "StartWaitingWorkflowsHandler",
    "RunTaskHandler",
    "CompleteTaskHandler",
    "CompleteStageHandler",
    "CompleteWorkflowHandler",
    "SkipStageHandler",
    "CancelStageHandler",
    "ContinueParentStageHandler",
    "JumpToStageHandler",
]


def register_all_handlers(
    processor: Any,
    repository: Any,
    task_registry: Any,
    queue: Any,
) -> None:
    """
    Register all handlers with a queue processor.

    .. deprecated::
        Pass ``store`` and ``task_registry`` to :class:`QueueProcessor` instead.
        Handlers are now auto-registered by the constructor.

    Args:
        processor: The queue processor to register with
        repository: The execution repository
        task_registry: The task registry
        queue: The message queue
    """
    import warnings

    warnings.warn(
        "register_all_handlers() is deprecated. Pass store and task_registry to QueueProcessor() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    handlers = [
        StartWorkflowHandler(queue, repository),
        StartWaitingWorkflowsHandler(queue, repository),
        StartStageHandler(queue, repository),
        SkipStageHandler(queue, repository),
        CancelStageHandler(queue, repository),
        ContinueParentStageHandler(queue, repository),
        JumpToStageHandler(queue, repository),
        StartTaskHandler(queue, repository, task_registry),
        RunTaskHandler(queue, repository, task_registry),
        CompleteTaskHandler(queue, repository),
        CompleteStageHandler(queue, repository),
        CompleteWorkflowHandler(queue, repository),
    ]

    for handler in handlers:
        processor.register_handler(handler)
