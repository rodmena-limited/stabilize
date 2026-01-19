"""
CancelStageHandler - handles stage cancellation.

This handler is invoked when a stage needs to be canceled, typically
due to upstream failure or execution cancellation.
"""

from __future__ import annotations

import logging
import random
import time
from typing import TYPE_CHECKING

from stabilize.errors import ConcurrencyError
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CancelStage

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)


class CancelStageHandler(StabilizeHandler[CancelStage]):
    """
    Handler for CancelStage messages.

    Execution flow:
    1. Check if stage is still in a cancellable state
    2. Cancel all running tasks
    3. Set stage status to CANCELED
    4. Set end time
    5. Store the stage
    6. Does NOT trigger downstream (cancellation terminates the path)
    """

    @property
    def message_type(self) -> type[CancelStage]:
        return CancelStage

    def handle(self, message: CancelStage) -> None:
        """Handle the CancelStage message.

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
                        "Failed to cancel stage after %d attempts due to contention (execution=%s, stage=%s)",
                        max_retries,
                        message.execution_id,
                        message.stage_id,
                    )
                    raise

                backoff = (0.1 * (2**attempt)) + (random.random() * 0.1)
                logger.warning(
                    "Concurrency error canceling stage, retrying in %.2fs (attempt %d/%d)",
                    backoff,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(backoff)

    def _handle_with_retry(self, message: CancelStage) -> None:
        """Inner handle logic to be retried."""

        def on_stage(stage: StageExecution) -> None:
            # Check if stage is still in a cancellable state
            if stage.status.is_complete:
                logger.debug(
                    "Ignoring CancelStage for %s (%s) - already %s",
                    stage.name,
                    stage.id,
                    stage.status,
                )
                return

            # Cancel all tasks that are still running
            for task in stage.tasks:
                if task.status in {WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING}:
                    self.set_task_status(task, WorkflowStatus.CANCELED)
                    task.end_time = self.current_time_millis()

            # Mark stage as canceled
            self.set_stage_status(stage, WorkflowStatus.CANCELED)
            stage.end_time = self.current_time_millis()

            # Atomic: store stage + message deduplication
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)

                # Message deduplication
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="CancelStage",
                        execution_id=message.execution_id,
                    )

            logger.info("Canceled stage %s (%s)", stage.name, stage.id)

            # Note: We do NOT call start_next() here because cancellation
            # terminates this path of execution. The CompleteWorkflow handler
            # will handle the final execution status determination.

        self.with_stage(message, on_stage)
