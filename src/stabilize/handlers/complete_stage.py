from __future__ import annotations
import logging
from typing import TYPE_CHECKING
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteStage,
    CompleteWorkflow,
    StartStage,
)
logger = logging.getLogger(__name__)

class CompleteStageHandler(StabilizeHandler[CompleteStage]):
    """
    Handler for CompleteStage messages.

    Execution flow:
    1. Check if stage already complete
    2. Determine status from synthetic stages and tasks
    3. If success: Plan and start after stages
    4. If failure: Plan and start on-failure stages
    5. Update stage status and end time
    6. If status allows continuation: startNext()
    7. Otherwise: CancelStage + CompleteWorkflow
    """
