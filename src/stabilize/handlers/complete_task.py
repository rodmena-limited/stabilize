from __future__ import annotations
import logging
from typing import TYPE_CHECKING
from stabilize.handlers.base import StabilizeHandler
from stabilize.queue.messages import (
    CompleteStage,
    CompleteTask,
    StartTask,
)
logger = logging.getLogger(__name__)

class CompleteTaskHandler(StabilizeHandler[CompleteTask]):
    """
    Handler for CompleteTask messages.

    Execution flow:
    1. Update task status and end time
    2. If there's a next task: Push StartTask
    3. Otherwise: Push CompleteStage
    """

    def message_type(self) -> type[CompleteTask]:
        return CompleteTask
