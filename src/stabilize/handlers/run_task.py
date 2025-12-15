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
