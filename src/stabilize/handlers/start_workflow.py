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
