"""
ContinueParentStageHandler - handles synthetic stage completion notification.

This handler is invoked when a synthetic stage (before/after stage) completes
and the parent stage needs to be notified to continue processing.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import CONTINUABLE_STATUSES, HALT_STATUSES, WorkflowStatus
from stabilize.queue.messages import (
    CompleteStage,
    ContinueParentStage,
    StartStage,
    StartTask,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)

# Maximum number of times to re-queue ContinueParentStage before giving up
# With a 15-second retry delay, 240 retries = 1 hour maximum wait
MAX_CONTINUE_PARENT_RETRIES = 240


class ContinueParentStageHandler(StabilizeHandler[ContinueParentStage]):
    """
    Handler for ContinueParentStage messages.

    Execution flow based on phase:

    For STAGE_BEFORE phase:
    1. Check if all before-stages are complete
    2. If complete: start the parent's first task
    3. If not: wait (another ContinueParentStage will be sent)

    For STAGE_AFTER phase:
    1. Check if all after-stages are complete
    2. If complete: push CompleteStage for parent
    3. If not: wait (another ContinueParentStage will be sent)
    """

    @property
    def message_type(self) -> type[ContinueParentStage]:
        return ContinueParentStage

    def handle(self, message: ContinueParentStage) -> None:
        """Handle the ContinueParentStage message."""

        def on_stage(stage: StageExecution) -> None:
            phase = message.phase

            if phase == SyntheticStageOwner.STAGE_BEFORE:
                self._handle_before_phase(stage, message)
            elif phase == SyntheticStageOwner.STAGE_AFTER:
                self._handle_after_phase(stage, message)
            else:
                logger.warning(
                    "Unknown synthetic stage phase %s for parent %s",
                    phase,
                    stage.id,
                )

        self.with_stage(message, on_stage)

    def _handle_before_phase(
        self,
        stage: StageExecution,
        message: ContinueParentStage,
    ) -> None:
        """Handle completion of before-stages."""
        # Get all before-stages for this parent
        before_stages = stage.before_stages()

        # Check if all before-stages are complete with continuable status
        all_complete = all(s.status in CONTINUABLE_STATUSES for s in before_stages)

        # Check if any failed with a halt status (TERMINAL, CANCELED, STOPPED)
        any_failed = any(s.status in HALT_STATUSES for s in before_stages)

        if any_failed:
            # Before-stage failed - mark parent as failed too using atomic transaction
            logger.warning(
                "Before-stage failed for parent %s (%s), marking as failed",
                stage.name,
                stage.id,
            )
            self.set_stage_status(stage, WorkflowStatus.TERMINAL)
            stage.end_time = self.current_time_millis()
            # Use atomic transaction to ensure state and message are committed together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                txn.push_message(
                    CompleteStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=stage.id,
                    )
                )
            return

        if not all_complete:
            # Not all before-stages complete yet - check retry count
            retry_count = message.retry_count or 0

            if retry_count >= MAX_CONTINUE_PARENT_RETRIES:
                logger.error(
                    "ContinueParentStage for %s (%s) exceeded max retries (%d). "
                    "Before-stages may be stuck. Marking parent as TERMINAL.",
                    stage.name,
                    stage.id,
                    MAX_CONTINUE_PARENT_RETRIES,
                )
                self.set_stage_status(stage, WorkflowStatus.TERMINAL)
                stage.end_time = self.current_time_millis()
                stage.context["exception"] = {
                    "details": {"error": "Exceeded max retries waiting for before-stages"},
                }
                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    txn.push_message(
                        CompleteStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.id,
                        )
                    )
                return

            # Re-queue with incremented retry count
            logger.debug(
                "Re-queuing ContinueParentStage for %s (%s) (retry %d/%d) - before-stages not complete",
                stage.name,
                stage.id,
                retry_count + 1,
                MAX_CONTINUE_PARENT_RETRIES,
            )
            new_message = ContinueParentStage(
                execution_type=message.execution_type,
                execution_id=message.execution_id,
                stage_id=message.stage_id,
                phase=message.phase,
                retry_count=retry_count + 1,
            )
            self.queue.push(new_message, self.retry_delay)
            return

        # All before-stages complete - start parent's first task
        logger.debug(
            "All before-stages complete for %s (%s), starting tasks",
            stage.name,
            stage.id,
        )

        # Use atomic transaction for message queuing
        first_task = stage.first_task()
        if first_task:
            with self.repository.transaction(self.queue) as txn:
                txn.push_message(
                    StartTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=stage.id,
                        task_id=first_task.id,
                    )
                )
        else:
            # No tasks - check for after-stages or complete
            after_stages = stage.first_after_stages()
            if after_stages:
                # Only push StartStage for after-stages that are NOT_STARTED to prevent duplicates
                not_started_after = [s for s in after_stages if s.status == WorkflowStatus.NOT_STARTED]
                if not_started_after:
                    with self.repository.transaction(self.queue) as txn:
                        for after in not_started_after:
                            txn.push_message(
                                StartStage(
                                    execution_type=message.execution_type,
                                    execution_id=message.execution_id,
                                    stage_id=after.id,
                                )
                            )
                # If after-stages exist but are already running/complete, don't complete stage yet
                # They will trigger ContinueParentStage when done
            else:
                # No tasks, no after-stages - complete stage
                with self.repository.transaction(self.queue) as txn:
                    txn.push_message(
                        CompleteStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.id,
                        )
                    )

    def _handle_after_phase(
        self,
        stage: StageExecution,
        message: ContinueParentStage,
    ) -> None:
        """Handle completion of after-stages."""
        # Get all after-stages for this parent
        after_stages = stage.after_stages()

        # Check if all after-stages are complete with continuable status
        all_complete = all(s.status in CONTINUABLE_STATUSES for s in after_stages)

        # Check if any failed with a halt status (TERMINAL, CANCELED, STOPPED)
        any_failed = any(s.status in HALT_STATUSES for s in after_stages)

        if any_failed:
            # After-stage failed - mark parent as failed using atomic transaction
            logger.warning(
                "After-stage failed for parent %s (%s), marking as failed",
                stage.name,
                stage.id,
            )
            self.set_stage_status(stage, WorkflowStatus.TERMINAL)
            stage.end_time = self.current_time_millis()
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                txn.push_message(
                    CompleteStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=stage.id,
                    )
                )
            return

        if not all_complete:
            # Not all after-stages complete yet - check retry count
            retry_count = message.retry_count or 0

            if retry_count >= MAX_CONTINUE_PARENT_RETRIES:
                logger.error(
                    "ContinueParentStage for %s (%s) exceeded max retries (%d). "
                    "After-stages may be stuck. Marking parent as TERMINAL.",
                    stage.name,
                    stage.id,
                    MAX_CONTINUE_PARENT_RETRIES,
                )
                self.set_stage_status(stage, WorkflowStatus.TERMINAL)
                stage.end_time = self.current_time_millis()
                stage.context["exception"] = {
                    "details": {"error": "Exceeded max retries waiting for after-stages"},
                }
                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    txn.push_message(
                        CompleteStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.id,
                        )
                    )
                return

            # Re-queue with incremented retry count
            logger.debug(
                "Re-queuing ContinueParentStage for %s (%s) (retry %d/%d) - after-stages not complete",
                stage.name,
                stage.id,
                retry_count + 1,
                MAX_CONTINUE_PARENT_RETRIES,
            )
            new_message = ContinueParentStage(
                execution_type=message.execution_type,
                execution_id=message.execution_id,
                stage_id=message.stage_id,
                phase=message.phase,
                retry_count=retry_count + 1,
            )
            self.queue.push(new_message, self.retry_delay)
            return

        # All after-stages complete successfully - complete the parent stage
        logger.debug(
            "All after-stages complete for %s (%s), completing parent",
            stage.name,
            stage.id,
        )

        # Push CompleteStage to finalize using atomic transaction
        with self.repository.transaction(self.queue) as txn:
            txn.push_message(
                CompleteStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=stage.id,
                )
            )
