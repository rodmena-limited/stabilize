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

from resilient_circuit import ExponentialDelay, RetryWithBackoffPolicy
from resilient_circuit.exceptions import RetryLimitReached

from stabilize.errors import ConcurrencyError
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus, validate_transition
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowNotFoundError
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
from stabilize.resilience.config import HandlerConfig, get_handler_config

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

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

    Attributes:
        queue: The message queue for pushing messages
        repository: The workflow store for persistence
        retry_delay: Delay before re-queuing messages (from config or override)
        handler_config: Configuration for retry behavior and other settings
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        self.queue = queue
        self.repository = repository
        self.handler_config = handler_config or get_handler_config()
        # Use explicit retry_delay if provided, otherwise use config
        self.retry_delay = retry_delay or timedelta(seconds=self.handler_config.handler_retry_delay_seconds)

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
        except WorkflowNotFoundError:
            logger.error("Execution not found: %s", message.execution_id)
            # Use atomic transaction with deduplication to prevent infinite retries
            with self.repository.transaction(self.queue) as txn:
                if hasattr(message, "message_id") and message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="with_execution",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    InvalidWorkflowId(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                    )
                )
            return
        except Exception as e:
            logger.error("Failed to retrieve execution %s: %s", message.execution_id, e)
            raise

        block(execution)

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
        except ValueError:
            logger.error("Stage not found: %s", message.stage_id)
            # Use atomic transaction with deduplication to prevent infinite retries
            with self.repository.transaction(self.queue) as txn:
                if hasattr(message, "message_id") and message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="with_stage",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    InvalidStageId(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                    )
                )
            return
        except Exception as e:
            logger.error("Failed to retrieve stage %s: %s", message.stage_id, e)
            raise

        block(stage)

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
                # Use atomic transaction with deduplication to prevent infinite retries
                with self.repository.transaction(self.queue) as txn:
                    if hasattr(message, "message_id") and message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="with_task",
                            execution_id=message.execution_id,
                        )
                    txn.push_message(
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
            # Use atomic transaction to ensure message is persisted with stage state
            parent_id = stage.parent_stage_id
            if parent_id:
                with self.repository.transaction(self.queue) as txn:
                    txn.push_message(
                        ContinueParentStage(
                            execution_type=execution.type.value,
                            execution_id=execution.id,
                            stage_id=parent_id,
                            phase=phase,
                        )
                    )
            else:
                logger.error(
                    "Synthetic stage %s has phase=%s but no parent_stage_id. "
                    "Data inconsistency - completing workflow to prevent hang. Execution: %s",
                    stage.id,
                    phase,
                    execution.id,
                )
                # Complete workflow to prevent hang from data inconsistency
                self.queue.push(
                    CompleteWorkflow(
                        execution_type=execution.type.value,
                        execution_id=execution.id,
                    )
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

    def retry_on_concurrency_error(
        self,
        func: Callable[[], None],
        context: str = "operation",
    ) -> None:
        """Execute a function with retry on ConcurrencyError.

        Uses resilient-circuit's RetryWithBackoffPolicy for consistent retry behavior.
        Configuration comes from handler_config settings.
        Set concurrency_max_retries to 0 to disable retries entirely.

        Args:
            func: The function to execute
            context: Description for logging (e.g., "completing task", "starting stage")

        Raises:
            ConcurrencyError: If max retries exceeded
        """
        max_retries = self.handler_config.concurrency_max_retries

        # If retries disabled, just run once
        if max_retries == 0:
            func()
            return

        # Create backoff configuration from handler_config
        concurrency_backoff = ExponentialDelay(
            min_delay=timedelta(milliseconds=self.handler_config.concurrency_min_delay_ms),
            max_delay=timedelta(milliseconds=self.handler_config.concurrency_max_delay_ms),
            factor=int(self.handler_config.concurrency_backoff_factor),
            jitter=self.handler_config.concurrency_jitter,
        )

        # Create retry policy with ConcurrencyError handling
        retry_policy = RetryWithBackoffPolicy(
            max_retries=max_retries,
            backoff=concurrency_backoff,
            should_handle=lambda e: isinstance(e, ConcurrencyError),
        )

        @retry_policy
        def with_retry() -> None:
            func()

        try:
            with_retry()
        except RetryLimitReached as e:
            logger.error(
                "Failed %s after %d attempts due to contention",
                context,
                max_retries,
            )
            # Re-raise the original ConcurrencyError
            if e.__cause__ and isinstance(e.__cause__, ConcurrencyError):
                raise e.__cause__ from e
            raise ConcurrencyError(f"Max retries exceeded for {context}") from e

    # ========== State Transition Helpers ==========

    def set_stage_status(
        self,
        stage: StageExecution,
        new_status: WorkflowStatus,
    ) -> None:
        """Set stage status with validation.

        Validates the transition is allowed before setting the status.

        Args:
            stage: The stage to update
            new_status: The new status to set

        Raises:
            InvalidStateTransitionError: If the transition is not allowed
        """
        validate_transition(
            stage.status,
            new_status,
            entity_type="stage",
            entity_id=stage.id,
        )
        stage.status = new_status

    def set_task_status(
        self,
        task: TaskExecution,
        new_status: WorkflowStatus,
    ) -> None:
        """Set task status with validation.

        Validates the transition is allowed before setting the status.

        Args:
            task: The task to update
            new_status: The new status to set

        Raises:
            InvalidStateTransitionError: If the transition is not allowed
        """
        validate_transition(
            task.status,
            new_status,
            entity_type="task",
            entity_id=task.id,
        )
        task.status = new_status

    def set_workflow_status(
        self,
        workflow: Workflow,
        new_status: WorkflowStatus,
    ) -> None:
        """Set workflow status with validation.

        Validates the transition is allowed before setting the status.

        Args:
            workflow: The workflow to update
            new_status: The new status to set

        Raises:
            InvalidStateTransitionError: If the transition is not allowed
        """
        validate_transition(
            workflow.status,
            new_status,
            entity_type="workflow",
            entity_id=workflow.id,
        )
        workflow.status = new_status
