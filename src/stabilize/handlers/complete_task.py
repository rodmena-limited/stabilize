"""
CompleteTaskHandler - handles task completion.

This handler updates task status and triggers either the next task
or stage completion.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteStage,
    CompleteTask,
    StartTask,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class CompleteTaskHandler(StabilizeHandler[CompleteTask]):
    """
    Handler for CompleteTask messages.

    Execution flow:
    1. Update task status and end time
    2. If there's a next task: Push StartTask
    3. Otherwise: Push CompleteStage
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config)

    @property
    def message_type(self) -> type[CompleteTask]:
        return CompleteTask

    def handle(self, message: CompleteTask) -> None:
        """Handle the CompleteTask message.

        Uses atomic transactions to ensure stage updates and message pushes
        are committed together, preventing orphaned states.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"completing task {message.task_id}",
        )

    def _handle_with_retry(self, message: CompleteTask) -> None:
        """Inner handle logic to be retried."""

        def on_task(stage: StageExecution, task: TaskExecution) -> None:
            # Idempotency check - only complete tasks that are RUNNING
            if task.status != WorkflowStatus.RUNNING:
                logger.debug(
                    "Ignoring CompleteTask for %s (%s) - already %s",
                    task.name,
                    task.id,
                    task.status,
                )
                # Mark message as processed to prevent infinite reprocessing
                if message.message_id:
                    with self.repository.transaction(self.queue) as txn:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="CompleteTask",
                            execution_id=message.execution_id,
                        )
                return

            # Update task status
            self.set_task_status(task, message.status)
            task.end_time = self.current_time_millis()

            logger.debug("Task %s completed with status %s", task.name, message.status)

            # For REDIRECT status, the task initiated a jump to another stage.
            # JumpToStageHandler handles the flow control, so we just complete
            # the task and don't start the next task or complete the stage.
            if message.status == WorkflowStatus.REDIRECT:
                logger.debug(
                    "Task %s completed with REDIRECT - flow handled by JumpToStageHandler",
                    task.name,
                )
                # Atomic: store stage only (no next message)
                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="CompleteTask",
                            execution_id=message.execution_id,
                        )
                return

            # Check for next task
            next_task = stage.next_task(task)

            # Atomic: store stage + push next message together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)

                # Atomic deduplication
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="CompleteTask",
                        execution_id=message.execution_id,
                    )

                if next_task is not None:
                    txn.push_message(
                        StartTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=next_task.id,
                        )
                    )
                else:
                    # No more tasks - complete stage
                    txn.push_message(
                        CompleteStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                        )
                    )

        self.with_task(message, on_task)
