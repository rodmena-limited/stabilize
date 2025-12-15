from __future__ import annotations
import logging
from typing import TYPE_CHECKING
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteStage,
    CompleteWorkflow,
    SkipStage,
    StartStage,
    StartTask,
)
logger = logging.getLogger(__name__)

class StartStageHandler(StabilizeHandler[StartStage]):
    """
    Handler for StartStage messages.

    Execution flow:
    1. Check if any upstream stages failed -> CompleteWorkflow
    2. Check if all upstream stages complete
       - If not: Re-queue with retry delay
       - If yes: Continue to step 3
    3. Check if stage should be skipped -> SkipStage
    4. Check if start time expired -> SkipStage
    5. Plan the stage (build tasks and before stages)
    6. Start the stage:
       - If has before stages: StartStage for each
       - Else if has tasks: StartTask for first task
       - Else: CompleteStage
    """

    def message_type(self) -> type[StartStage]:
        return StartStage

    def handle(self, message: StartStage) -> None:
        """Handle the StartStage message."""

        def on_stage(stage: StageExecution) -> None:
            try:
                # Get upstream stages from repository
                upstream_stages = self.repository.get_upstream_stages(stage.execution.id, stage.ref_id)

                # Check if any upstream stages failed
                # We check for terminal statuses in direct dependencies
                halt_statuses = {
                    WorkflowStatus.TERMINAL,
                    WorkflowStatus.STOPPED,
                    WorkflowStatus.CANCELED,
                }

                upstream_failed = False
                for upstream in upstream_stages:
                    if upstream.status in halt_statuses:
                        upstream_failed = True
                        break

                if upstream_failed:
                    logger.warning(
                        "Upstream stage failed for %s (%s), completing execution",
                        stage.name,
                        stage.id,
                    )
                    self.queue.push(
                        CompleteWorkflow(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                        )
                    )
                    return

                # Check if all upstream stages are complete
                # CONTINUABLE_STATUSES = {SUCCEEDED, FAILED_CONTINUE, SKIPPED}
                # We can't import CONTINUABLE_STATUSES easily inside here without circular imports potentially,
                # but it is available in models.status
                from stabilize.models.status import CONTINUABLE_STATUSES

                all_complete = True
                for upstream in upstream_stages:
                    if upstream.status not in CONTINUABLE_STATUSES:
                        all_complete = False
                        break

                if all_complete:
                    self._start_if_ready(stage, message)
                else:
                    # Upstream not complete - re-queue
                    logger.debug(
                        "Re-queuing %s (%s) - upstream stages not complete",
                        stage.name,
                        stage.id,
                    )
                    self.queue.push(message, self.retry_delay)

            except Exception as e:
                logger.error(
                    "Error starting stage %s (%s): %s",
                    stage.name,
                    stage.id,
                    e,
                    exc_info=True,
                )
                stage.context["exception"] = {
                    "details": {"error": str(e)},
                }
                stage.context["beforeStagePlanningFailed"] = True
                self.repository.store_stage(stage)
                self.queue.push(
                    CompleteStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                    )
                )

        self.with_stage(message, on_stage)
