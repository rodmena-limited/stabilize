from __future__ import annotations
import logging
from typing import TYPE_CHECKING
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import CONTINUABLE_STATUSES, WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteWorkflow,
    StartWaitingWorkflows,
)
logger = logging.getLogger(__name__)

class CompleteWorkflowHandler(StabilizeHandler[CompleteWorkflow]):
    """
    Handler for CompleteWorkflow messages.

    Execution flow:
    1. Check if execution already complete
    2. Determine final status from top-level stages
    3. Update execution status
    4. Cancel any running stages if failed
    5. Start waiting executions if queue is enabled
    """

    def message_type(self) -> type[CompleteWorkflow]:
        return CompleteWorkflow
