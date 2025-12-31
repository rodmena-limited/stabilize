"""
Base message handler classes.

This module provides the base classes for all message handlers in the
pipeline execution engine.
"""

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

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue

logger = logging.getLogger(__name__)

M = TypeVar("M", bound=Message)


class MessageHandler(ABC, Generic[M]):
    """
    Base class for message handlers.

    Each handler processes a specific type of message.
    """

    @property
    @abstractmethod
    def message_type(self) -> type[M]:
        """Return the type of message this handler processes."""
        pass

    @abstractmethod
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

    # ========== Execution Retrieval ==========

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
            # Should we retry or fail? For now, log error.

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

    def _find_task(
        self,
        stage: StageExecution,
        task_id: str,
    ) -> TaskExecution | None:
        """Find a task by ID in a stage."""
        for task in stage.tasks:
            if task.id == task_id:
                return task
        return None

    # ========== Stage Navigation ==========

    def start_next(self, stage: StageExecution) -> None:
        """
        Start the next stage(s) after a stage completes.

        This is the critical method for DAG traversal:
        1. Find downstream stages (those that depend on this stage)
        2. Push StartStage for each downstream stage
        3. If this is a synthetic stage, notify parent
        4. If no downstream and not synthetic, complete execution
        """
        execution = stage.execution
        downstream_stages = self.repository.get_downstream_stages(execution.id, stage.ref_id)
        phase = stage.synthetic_stage_owner

        if downstream_stages:
            # Start all downstream stages
            for downstream in downstream_stages:
                self.queue.push(
                    StartStage(
                        execution_type=execution.type.value,
                        execution_id=execution.id,
                        stage_id=downstream.id,
                    )
                )
        elif phase is not None:
            # Synthetic stage - notify parent
            parent_id = stage.parent_stage_id
            if parent_id:
                self.queue.ensure(
                    ContinueParentStage(
                        execution_type=execution.type.value,
                        execution_id=execution.id,
                        stage_id=parent_id,
                        phase=phase,
                    ),
                    timedelta(seconds=0),
                )
        else:
            # Top-level stage with no downstream - complete execution
            self.queue.push(
                CompleteWorkflow(
                    execution_type=execution.type.value,
                    execution_id=execution.id,
                )
            )

    # ========== Utility Methods ==========

    def current_time_millis(self) -> int:
        """Get current time in milliseconds."""
        return int(time.time() * 1000)
