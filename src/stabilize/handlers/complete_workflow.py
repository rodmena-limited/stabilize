from __future__ import annotations
import logging
from typing import TYPE_CHECKING
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import CONTINUABLE_STATUSES, WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteWorkflow,
    StartWaitingWorkflows,
)
logger = logging.getLogger(__name__)

class CompleteWorkflowHandler(StabilizeHandler[CompleteWorkflow]):
    """
    Handler for CompleteWorkflow messages.

    Execution flow:
    1. Check if execution already complete
    2. Determine final status from top-level stages
    3. Update execution status
    4. Cancel any running stages if failed
    5. Start waiting executions if queue is enabled
    """

    def message_type(self) -> type[CompleteWorkflow]:
        return CompleteWorkflow

    def handle(self, message: CompleteWorkflow) -> None:
        """Handle the CompleteWorkflow message."""

        def on_execution(execution: Workflow) -> None:
            # Check if already complete
            if execution.status.is_complete:
                logger.debug(
                    "Execution %s already complete with status %s",
                    execution.id,
                    execution.status,
                )
                return

            # Determine final status
            status = self._determine_final_status(execution, message)
            if status is None:
                # Not ready to complete - stages still running
                return

            # Update execution
            execution.status = status
            execution.end_time = self.current_time_millis()
            self.repository.update_status(execution)

            logger.info("Execution %s completed with status %s", execution.id, status)

            # Cancel any running stages if not successful
            if status != WorkflowStatus.SUCCEEDED:
                running_stages = [s for s in execution.top_level_stages() if s.status == WorkflowStatus.RUNNING]
                for stage in running_stages:
                    self.queue.push(
                        CancelStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.id,
                        )
                    )

            # Start waiting executions if configured
            if execution.status != WorkflowStatus.RUNNING and execution.pipeline_config_id:
                self.queue.push(
                    StartWaitingWorkflows(
                        pipeline_config_id=execution.pipeline_config_id,
                        purge_queue=not execution.keep_waiting_pipelines,
                    )
                )

        self.with_execution(message, on_execution)
