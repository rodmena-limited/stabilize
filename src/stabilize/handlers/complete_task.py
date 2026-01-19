"""
CompleteTaskHandler - handles task completion.

This handler updates task status and triggers either the next task
or stage completion.
"""

from __future__ import annotations

import logging
import random
import time
from typing import TYPE_CHECKING

from stabilize.errors import ConcurrencyError
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteStage,
    CompleteTask,
    StartTask,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution

logger = logging.getLogger(__name__)


class CompleteTaskHandler(StabilizeHandler[CompleteTask]):
    """
    Handler for CompleteTask messages.

    Execution flow:
    1. Update task status and end time
    2. If there's a next task: Push StartTask
    3. Otherwise: Push CompleteStage
    """

    @property
    def message_type(self) -> type[CompleteTask]:
        return CompleteTask

    def handle(self, message: CompleteTask) -> None:
        """Handle the CompleteTask message.

        Uses atomic transactions to ensure stage updates and message pushes
        are committed together, preventing orphaned states.

        Retries on ConcurrencyError (optimistic lock failure).
        """
        max_retries = 3

        for attempt in range(max_retries + 1):
            try:
                self._handle_with_retry(message)
                return
            except ConcurrencyError:
                if attempt == max_retries:
                    logger.error(
                        "Failed to update task after %d attempts due to contention (execution=%s, stage=%s, task=%s)",
                        max_retries,
                        message.execution_id,
                        message.stage_id,
                        message.task_id,
                    )
                    raise

                # Randomized exponential backoff
                backoff = (0.1 * (2**attempt)) + (random.random() * 0.1)
                logger.warning(
                    "Concurrency error updating task, retrying in %.2fs (attempt %d/%d)",
                    backoff,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(backoff)

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
