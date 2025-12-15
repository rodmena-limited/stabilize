from __future__ import annotations
import logging
from datetime import timedelta
from typing import TYPE_CHECKING
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteTask,
    PauseTask,
    RunTask,
)
from stabilize.tasks.interface import RetryableTask, Task
from stabilize.tasks.registry import TaskNotFoundError, TaskRegistry
from stabilize.tasks.result import TaskResult
logger = logging.getLogger(__name__)

class TimeoutError(Exception):
    """Raised when a task times out."""

class RunTaskHandler(StabilizeHandler[RunTask]):
    """
    Handler for RunTask messages.

    This is where tasks are actually executed. The handler:
    1. Resolves the task implementation
    2. Checks for cancellation/pause
    3. Checks for timeout
    4. Executes the task
    5. Processes the result
    """
    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        task_registry: TaskRegistry,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        super().__init__(queue, repository, retry_delay)
        self.task_registry = task_registry

    def message_type(self) -> type[RunTask]:
        return RunTask
