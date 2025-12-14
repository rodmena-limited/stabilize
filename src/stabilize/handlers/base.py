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

    def with_execution(
        self,
        message: WorkflowLevel,
        block: Callable[[Workflow], None],
    ) -> None:
        """
        Execute a block with the execution for a message.

        Args:
            message: Message containing execution ID
            block: Function to call with the execution
        """
        try:
            execution = self.repository.retrieve(message.execution_id)
            block(execution)
        except Exception as e:
            logger.error("Failed to retrieve execution %s: %s", message.execution_id, e)
            self.queue.push(
                InvalidWorkflowId(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                )
            )

    def with_stage(
        self,
        message: StageLevel,
        block: Callable[[StageExecution], None],
    ) -> None:
        """
        Execute a block with the stage for a message.

        Args:
            message: Message containing stage ID
            block: Function to call with the stage
        """
        try:
            stage = self.repository.retrieve_stage(message.stage_id)
            block(stage)
        except ValueError:
            logger.error("Stage not found: %s", message.stage_id)
            self.queue.push(
                InvalidStageId(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                )
            )
        except Exception as e:
            logger.error("Failed to retrieve stage %s: %s", message.stage_id, e)

    def with_task(
        self,
        message: TaskLevel,
        block: Callable[[StageExecution, TaskExecution], None],
    ) -> None:
        """
        Execute a block with the stage and task for a message.

        Args:
            message: Message containing task ID
            block: Function to call with (stage, task)
        """

        def on_stage(stage: StageExecution) -> None:
            task = self._find_task(stage, message.task_id)
            if task is None:
                logger.error("Task not found: %s", message.task_id)
                self.queue.push(
                    InvalidTaskId(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                    )
                )
            else:
                block(stage, task)

        self.with_stage(message, on_stage)
