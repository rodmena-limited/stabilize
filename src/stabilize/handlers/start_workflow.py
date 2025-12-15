"""
StartWorkflowHandler - handles pipeline execution startup.

This handler is triggered when a new pipeline execution is started.
It finds initial stages (those with no dependencies) and queues them
for execution.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelWorkflow,
    StartStage,
    StartWorkflow,
)

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow

logger = logging.getLogger(__name__)


class StartWorkflowHandler(StabilizeHandler[StartWorkflow]):
    """
    Handler for StartWorkflow messages.

    When a pipeline execution starts:
    1. Check if execution should be queued (concurrent limits)
    2. Find initial stages (no dependencies)
    3. Push StartStage for each initial stage
    4. Mark execution as RUNNING
    """

    @property
    def message_type(self) -> type[StartWorkflow]:
        return StartWorkflow

    def handle(self, message: StartWorkflow) -> None:
        """Handle the StartWorkflow message."""

        def on_execution(execution: Workflow) -> None:
            # Check if already started or canceled
            if execution.status != WorkflowStatus.NOT_STARTED:
                logger.warning(
                    "Execution %s already has status %s, ignoring StartWorkflow",
                    execution.id,
                    execution.status,
                )
                return

            if execution.is_canceled:
                logger.info("Execution %s was canceled before start", execution.id)
                self._terminate(execution)
                return

            # Check if start time has expired
            if self._is_after_start_time_expiry(execution):
                logger.warning("Execution %s start time expired, canceling", execution.id)
                self.queue.push(
                    CancelWorkflow(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        user="system",
                        reason="Could not begin execution before start time expiry",
                    )
                )
                return

            # TODO: Check if should queue (concurrent execution limits)
            # if execution.should_queue():
            #     self.pending_execution_service.enqueue(execution.pipeline_config_id, message)
            #     return

            self._start(execution, message)

        self.with_execution(message, on_execution)

    def _start(
        self,
        execution: Workflow,
        message: StartWorkflow,
    ) -> None:
        """Start the execution."""
        initial_stages = execution.initial_stages()

        if not initial_stages:
            logger.warning("No initial stages found for execution %s", execution.id)
            execution.status = WorkflowStatus.TERMINAL
            self.repository.update_status(execution)
            # Publish ExecutionComplete event
            return

        # Mark as running
        execution.status = WorkflowStatus.RUNNING
        execution.start_time = self.current_time_millis()
        self.repository.update_status(execution)

        # Queue all initial stages
        for stage in initial_stages:
            logger.debug(
                "Queuing initial stage %s (%s) for execution %s",
                stage.name,
                stage.id,
                execution.id,
            )
            self.queue.push(
                StartStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=stage.id,
                )
            )

        logger.info(
            "Started execution %s with %d initial stage(s)",
            execution.id,
            len(initial_stages),
        )

    def _terminate(self, execution: Workflow) -> None:
        """Terminate a canceled execution."""
        # Publish ExecutionComplete event
        if execution.pipeline_config_id:
            # Queue start waiting executions
            pass

    def _is_after_start_time_expiry(self, execution: Workflow) -> bool:
        """Check if current time is past start time expiry."""
        if execution.start_time_expiry is None:
            return False
        return self.current_time_millis() > execution.start_time_expiry
