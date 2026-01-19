"""
SkipStageHandler - handles stage skipping.

This handler is invoked when a stage should be skipped due to conditional
execution or start time expiry.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import SkipStage

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)


class SkipStageHandler(StabilizeHandler[SkipStage]):
    """
    Handler for SkipStage messages.

    Execution flow:
    1. Check if stage is still in a skippable state
    2. Set stage status to SKIPPED
    3. Set end time
    4. Store the stage
    5. Trigger downstream stages via start_next()
    """

    @property
    def message_type(self) -> type[SkipStage]:
        return SkipStage

    def handle(self, message: SkipStage) -> None:
        """Handle the SkipStage message."""

        def on_stage(stage: StageExecution) -> None:
            # Check if stage is still in a skippable state
            if stage.status not in {WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING}:
                logger.debug(
                    "Ignoring SkipStage for %s (%s) - already %s",
                    stage.name,
                    stage.id,
                    stage.status,
                )
                return

            # Mark stage as skipped
            stage.status = WorkflowStatus.SKIPPED
            stage.end_time = self.current_time_millis()

            # Store the updated stage
            self.repository.store_stage(stage)

            logger.info("Skipped stage %s (%s)", stage.name, stage.id)

            # Trigger downstream stages
            self.start_next(stage)

        self.with_stage(message, on_stage)
