"""
SkipStageHandler - handles stage skipping.

This handler is invoked when a stage should be skipped due to conditional
execution or start time expiry.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteWorkflow,
    ContinueParentStage,
    SkipStage,
    StartStage,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

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

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config)

    @property
    def message_type(self) -> type[SkipStage]:
        return SkipStage

    def handle(self, message: SkipStage) -> None:
        """Handle the SkipStage message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"skipping stage {message.stage_id}",
        )

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
