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
