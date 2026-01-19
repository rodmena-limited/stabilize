"""
CancelStageHandler - handles stage cancellation.

This handler is invoked when a stage needs to be canceled, typically
due to upstream failure or execution cancellation.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

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
        """Handle the CancelStage message."""

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
                    task.status = WorkflowStatus.CANCELED
                    task.end_time = self.current_time_millis()

            # Mark stage as canceled
            stage.status = WorkflowStatus.CANCELED
            stage.end_time = self.current_time_millis()

            # Store the updated stage
            self.repository.store_stage(stage)

            logger.info("Canceled stage %s (%s)", stage.name, stage.id)

            # Note: We do NOT call start_next() here because cancellation
            # terminates this path of execution. The CompleteWorkflow handler
            # will handle the final execution status determination.

        self.with_stage(message, on_stage)
