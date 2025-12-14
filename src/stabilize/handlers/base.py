from __future__ import annotations
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Generic, TypeVar
from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import (
    CompleteWorkflow,
    ContinueParentStage,
    InvalidStageId,
    InvalidTaskId,
    InvalidWorkflowId,
    Message,
    StageLevel,
    StartStage,
    TaskLevel,
    WorkflowLevel,
)
logger = logging.getLogger(__name__)
M = TypeVar("M", bound=Message)

class MessageHandler(ABC, Generic[M]):
    """
    Base class for message handlers.

    Each handler processes a specific type of message.
    """

    def message_type(self) -> type[M]:
        """Return the type of message this handler processes."""
        pass

    def handle(self, message: M) -> None:
        """Handle a message."""
        pass

class StabilizeHandler(MessageHandler[M], ABC):
    """
    Base handler with common utilities.

    Provides helper methods for retrieving executions, stages, and tasks,
    as well as the startNext() implementation.
    """
    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        self.queue = queue
        self.repository = repository
        self.retry_delay = retry_delay
