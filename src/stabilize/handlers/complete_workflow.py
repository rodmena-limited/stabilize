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
