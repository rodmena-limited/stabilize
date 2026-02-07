"""
CompleteWorkflowHandler - handles execution completion.

This handler determines the final execution status based on all
top-level stages and marks the execution as complete.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import CONTINUABLE_STATUSES, WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteWorkflow,
    StartWaitingWorkflows,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

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

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder)

    @property
    def message_type(self) -> type[CompleteWorkflow]:
        return CompleteWorkflow

    def handle(self, message: CompleteWorkflow) -> None:
        """Handle the CompleteWorkflow message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"completing workflow {message.execution_id}",
        )

    def _handle_with_retry(self, message: CompleteWorkflow) -> None:
        """Inner handle logic to be retried."""

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

            # Update execution status
            self.set_workflow_status(execution, status)
            execution.end_time = self.current_time_millis()

            logger.info("Execution %s completed with status %s", execution.id, status)

            # Record event if event recorder is configured
            if self.event_recorder:
                self.set_event_context(execution.id)
                if status == WorkflowStatus.SUCCEEDED:
                    self.event_recorder.record_workflow_completed(
                        execution,
                        source_handler="CompleteWorkflowHandler",
                    )
                elif status == WorkflowStatus.CANCELED:
                    self.event_recorder.record_workflow_canceled(
                        execution,
                        source_handler="CompleteWorkflowHandler",
                    )
                else:
                    self.event_recorder.record_workflow_failed(
                        execution,
                        error=f"Workflow ended with status {status.name}",
                        source_handler="CompleteWorkflowHandler",
                    )

            # Collect running stages to cancel if not successful
            running_stages = []
            if status != WorkflowStatus.SUCCEEDED:
                running_stages = [s for s in execution.top_level_stages() if s.status == WorkflowStatus.RUNNING]

            # Save pipeline_config_id before cleanup
            pipeline_config_id = execution.pipeline_config_id
            keep_waiting_pipelines = execution.keep_waiting_pipelines

            # Atomic: update execution status + cancel stages + start waiting workflows
            with self.repository.transaction(self.queue) as txn:
                txn.update_workflow_status(execution)

                # Message deduplication
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="CompleteWorkflow",
                        execution_id=message.execution_id,
                    )

                # Cancel any running stages if not successful
                for stage in running_stages:
                    txn.push_message(
                        CancelStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.id,
                        )
                    )

                # Start waiting executions if configured
                if pipeline_config_id:
                    txn.push_message(
                        StartWaitingWorkflows(
                            pipeline_config_id=pipeline_config_id,
                            purge_queue=not keep_waiting_pipelines,
                        )
                    )

            # Clean up memory references after workflow completes
            # This breaks circular references to help garbage collection
            execution.cleanup()

        self.with_execution(message, on_execution)

    def _determine_final_status(
        self,
        execution: Workflow,
        message: CompleteWorkflow,
    ) -> WorkflowStatus | None:
        """
        Determine the final execution status.

        Returns None if execution is not ready to complete.
        """
        stages = execution.top_level_stages()
        statuses = [s.status for s in stages]

        # All succeeded/skipped/failed_continue -> SUCCEEDED
        if all(s in CONTINUABLE_STATUSES for s in statuses):
            return WorkflowStatus.SUCCEEDED

        # Any TERMINAL -> TERMINAL
        if WorkflowStatus.TERMINAL in statuses:
            return WorkflowStatus.TERMINAL

        # Any CANCELED -> CANCELED
        if WorkflowStatus.CANCELED in statuses:
            return WorkflowStatus.CANCELED

        # Any STOPPED and no other branches incomplete
        if WorkflowStatus.STOPPED in statuses:
            if not self._other_branches_incomplete(stages):
                # Check for override
                if self._should_override_success(execution):
                    return WorkflowStatus.TERMINAL
                return WorkflowStatus.SUCCEEDED

        # Still running - check retry count before re-queuing
        retry_count = getattr(message, "retry_count", 0) or 0
        max_retries = self.handler_config.max_stage_wait_retries

        if retry_count >= max_retries:
            logger.error(
                "CompleteWorkflow for %s exceeded max retries (%d). Stages stuck in statuses: %s. Marking as TERMINAL.",
                execution.id,
                max_retries,
                statuses,
            )
            return WorkflowStatus.TERMINAL

        # Re-queue with incremented retry count
        logger.debug(
            "Re-queuing CompleteWorkflow for %s (retry %d/%d) - stages not complete. Statuses: %s",
            execution.id,
            retry_count + 1,
            max_retries,
            statuses,
        )
        # Create new message with incremented retry count
        new_message = CompleteWorkflow(
            execution_type=message.execution_type,
            execution_id=message.execution_id,
            retry_count=retry_count + 1,
        )
        self.queue.push(new_message, self.retry_delay)
        return None

    def _other_branches_incomplete(self, stages: list[StageExecution]) -> bool:
        """Check if any other branches are incomplete."""
        for stage in stages:
            if stage.status == WorkflowStatus.RUNNING:
                return True
            if stage.status == WorkflowStatus.NOT_STARTED and stage.all_upstream_stages_complete():
                return True
        return False

    def _should_override_success(self, execution: Workflow) -> bool:
        """Check if success should be overridden to failure."""
        for stage in execution.stages:
            if stage.status == WorkflowStatus.STOPPED:
                if stage.context.get("completeOtherBranchesThenFail"):
                    return True
        return False
