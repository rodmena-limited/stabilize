from __future__ import annotations
import logging
from typing import TYPE_CHECKING
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    RunTask,
    StartTask,
)
logger = logging.getLogger(__name__)

class StartTaskHandler(StabilizeHandler[StartTask]):
    """
    Handler for StartTask messages.

    Execution flow:
    1. Check if task is enabled (SkippableTask)
       - If not: Push CompleteTask(SKIPPED)
    2. Set task status to RUNNING
    3. Set task start time
    4. Push RunTask
    """
