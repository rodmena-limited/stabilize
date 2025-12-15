from __future__ import annotations
import logging
from typing import TYPE_CHECKING
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelWorkflow,
    StartStage,
    StartWorkflow,
)
logger = logging.getLogger(__name__)

class StartWorkflowHandler(StabilizeHandler[StartWorkflow]):
    """
    Handler for StartWorkflow messages.

    When a pipeline execution starts:
    1. Check if execution should be queued (concurrent limits)
    2. Find initial stages (no dependencies)
    3. Push StartStage for each initial stage
    4. Mark execution as RUNNING
    """
