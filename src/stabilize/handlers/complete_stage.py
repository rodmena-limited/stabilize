"""
CompleteStageHandler - handles stage completion.

This is a critical handler that:
1. Determines stage status from tasks and synthetic stages
2. Plans and starts after stages
3. Plans and starts on-failure stages
4. Triggers downstream stages via startNext()
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteStage,
    CompleteWorkflow,
    StartStage,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)


class CompleteStageHandler(StabilizeHandler[CompleteStage]):
    """
    Handler for CompleteStage messages.

    Execution flow:
    1. Check if stage already complete
    2. Determine status from synthetic stages and tasks
    3. If success: Plan and start after stages
    4. If failure: Plan and start on-failure stages
    5. Update stage status and end time
    6. If status allows continuation: startNext()
    7. Otherwise: CancelStage + CompleteWorkflow
    """

    @property
    def message_type(self) -> type[CompleteStage]:
        return CompleteStage

    def handle(self, message: CompleteStage) -> None:
        """Handle the CompleteStage message."""

        def on_stage(stage: StageExecution) -> None:
            # Check if already complete
            if stage.status not in {WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED}:
                logger.debug(
                    "Stage %s already has status %s, ignoring CompleteStage",
                    stage.name,
                    stage.status,
                )
                return

            try:
                # Determine status from tasks and synthetic stages
                status = stage.determine_status()

                # Handle after stages
                if status.is_complete and not status.is_halt:
                    after_stages = stage.first_after_stages()
                    if not after_stages:
                        self._plan_after_stages(stage)
                        after_stages = stage.first_after_stages()

                    not_started = [s for s in after_stages if s.status == WorkflowStatus.NOT_STARTED]
                    if not_started:
                        # Atomic: push all after stage messages together
                        with self.repository.transaction(self.queue) as txn:
                            for s in not_started:
                                txn.push_message(
                                    StartStage(
                                        execution_type=message.execution_type,
                                        execution_id=message.execution_id,
                                        stage_id=s.id,
                                    )
                                )
                        return

                    # If status is NOT_STARTED with no after stages, it's weird
                    if status == WorkflowStatus.NOT_STARTED:
                        logger.warning("Stage %s had no tasks or synthetic stages", stage.name)
                        status = WorkflowStatus.SKIPPED

                # Handle failure - plan on-failure stages
                elif status.is_failure:
                    has_on_failure = self._plan_on_failure_stages(stage)
                    if has_on_failure:
                        after_stages = stage.first_after_stages()
                        # Atomic: push all on-failure stage messages together
                        with self.repository.transaction(self.queue) as txn:
                            for s in after_stages:
                                txn.push_message(
                                    StartStage(
                                        execution_type=message.execution_type,
                                        execution_id=message.execution_id,
                                        stage_id=s.id,
                                    )
                                )
                        return

                # Update stage status
                stage.status = status
                stage.end_time = self.current_time_millis()

                logger.info("Stage %s completed with status %s", stage.name, status)

                # Handle FAILED_CONTINUE propagation to parent
                if (
                    status == WorkflowStatus.FAILED_CONTINUE
                    and stage.synthetic_stage_owner is not None
                    and not stage.allow_sibling_stages_to_continue_on_failure
                    and stage.parent_stage_id is not None
                ):
                    # Atomic: store stage + propagate failure to parent
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
                        txn.push_message(
                            CompleteStage(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=stage.parent_stage_id,
                            )
                        )
                elif status in {
                    WorkflowStatus.SUCCEEDED,
                    WorkflowStatus.FAILED_CONTINUE,
                    WorkflowStatus.SKIPPED,
                }:
                    # Store stage first, then continue to downstream stages
                    self.repository.store_stage(stage)
                    self.start_next(stage)
                else:
                    # Failure - atomic: store stage + cancel + complete
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
                        txn.push_message(
                            CancelStage(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                            )
                        )
                        if stage.synthetic_stage_owner is None or stage.parent_stage_id is None:
                            txn.push_message(
                                CompleteWorkflow(
                                    execution_type=message.execution_type,
                                    execution_id=message.execution_id,
                                )
                            )
                        else:
                            # Propagate to parent
                            txn.push_message(
                                CompleteStage(
                                    execution_type=message.execution_type,
                                    execution_id=message.execution_id,
                                    stage_id=stage.parent_stage_id,
                                )
                            )

            except Exception as e:
                logger.error(
                    "Error completing stage %s: %s",
                    stage.name,
                    e,
                    exc_info=True,
                )
                stage.context["exception"] = {
                    "details": {"error": str(e)},
                }
                stage.status = WorkflowStatus.TERMINAL
                stage.end_time = self.current_time_millis()

                # Atomic: store stage + cancel + complete workflow
                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    txn.push_message(
                        CancelStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                        )
                    )
                    txn.push_message(
                        CompleteWorkflow(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                        )
                    )

        self.with_stage(message, on_stage)

    def _plan_after_stages(self, stage: StageExecution) -> None:
        """
        Plan after stages using the stage definition builder.

        TODO: Implement StageDefinitionBuilder integration
        """
        # For now, do nothing - after stages should be pre-defined
        pass

    def _plan_on_failure_stages(self, stage: StageExecution) -> bool:
        """
        Plan on-failure stages using the stage definition builder.

        TODO: Implement StageDefinitionBuilder integration

        Returns:
            True if on-failure stages were added
        """
        # For now, return False - no on-failure stages
        return False
