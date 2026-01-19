"""
SkipStageHandler - handles stage skipping.

This handler is invoked when a stage should be skipped due to conditional
execution or start time expiry.
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
    CompleteWorkflow,
    ContinueParentStage,
    SkipStage,
    StartStage,
)

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
    4. Atomically store the stage and trigger downstream stages
    """

    @property
    def message_type(self) -> type[SkipStage]:
        return SkipStage

    def handle(self, message: SkipStage) -> None:
        """Handle the SkipStage message.

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
                        "Failed to skip stage after %d attempts due to contention (execution=%s, stage=%s)",
                        max_retries,
                        message.execution_id,
                        message.stage_id,
                    )
                    raise

                backoff = (0.1 * (2**attempt)) + (random.random() * 0.1)
                logger.warning(
                    "Concurrency error skipping stage, retrying in %.2fs (attempt %d/%d)",
                    backoff,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(backoff)

    def _handle_with_retry(self, message: SkipStage) -> None:
        """Inner handle logic to be retried."""

        def on_stage(stage: StageExecution) -> None:
            # Check if stage is still in a skippable state
            # Only NOT_STARTED stages can be skipped - RUNNING stages should be CANCELED
            # (VALID_TRANSITIONS doesn't allow RUNNING -> SKIPPED)
            if stage.status != WorkflowStatus.NOT_STARTED:
                logger.debug(
                    "Ignoring SkipStage for %s (%s) - already %s (only NOT_STARTED can be skipped)",
                    stage.name,
                    stage.id,
                    stage.status,
                )
                return

            # Mark stage as skipped
            self.set_stage_status(stage, WorkflowStatus.SKIPPED)
            stage.end_time = self.current_time_millis()

            logger.info("Skipped stage %s (%s)", stage.name, stage.id)

            # Get downstream stages and parent info BEFORE transaction
            execution = stage.execution
            downstream_stages = self.repository.get_downstream_stages(execution.id, stage.ref_id)
            phase = stage.synthetic_stage_owner

            # Atomic: store stage + push all downstream/parent messages together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)

                # Message deduplication
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="SkipStage",
                        execution_id=message.execution_id,
                    )

                if downstream_stages:
                    # Start all downstream stages
                    for downstream in downstream_stages:
                        txn.push_message(
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
                        txn.push_message(
                            ContinueParentStage(
                                execution_type=execution.type.value,
                                execution_id=execution.id,
                                stage_id=parent_id,
                                phase=phase,
                            )
                        )
                else:
                    # Terminal stage - complete workflow
                    txn.push_message(
                        CompleteWorkflow(
                            execution_type=execution.type.value,
                            execution_id=execution.id,
                        )
                    )

        self.with_stage(message, on_stage)
