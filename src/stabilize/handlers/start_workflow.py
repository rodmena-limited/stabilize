"""
StartWorkflowHandler - handles pipeline execution startup.

This handler is triggered when a new pipeline execution is started.
It finds initial stages (those with no dependencies) and queues them
for execution.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.audit import audit
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowCriteria
from stabilize.queue.messages import (
    CancelWorkflow,
    StartStage,
    StartWorkflow,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue

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

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config)

    @property
    def message_type(self) -> type[StartWorkflow]:
        return StartWorkflow

    def handle(self, message: StartWorkflow) -> None:
        """Handle the StartWorkflow message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"starting workflow {message.execution_id}",
        )

    def _handle_with_retry(self, message: StartWorkflow) -> None:
        """Inner handle logic to be retried."""

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

            # Check if should queue (concurrent execution limits)
            if self._should_queue(execution):
                logger.info(
                    "Execution %s queued due to concurrent execution limit (limit=%d)",
                    execution.id,
                    execution.max_concurrent_executions,
                )
                self.set_workflow_status(execution, WorkflowStatus.BUFFERED)
                # Atomic: update execution status + message deduplication
                with self.repository.transaction(self.queue) as txn:
                    txn.update_workflow_status(execution)
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="StartWorkflow",
                            execution_id=message.execution_id,
                        )
                return

            self._start(execution, message)

        self.with_execution(message, on_execution)

    def _should_queue(self, execution: Workflow) -> bool:
        """Check if execution should be queued due to concurrency limits."""
        if not execution.is_limit_concurrent or not execution.pipeline_config_id:
            return False

        if execution.max_concurrent_executions <= 0:
            return False

        # Count currently running executions for this pipeline config
        criteria = WorkflowCriteria(
            statuses={WorkflowStatus.RUNNING},
            page_size=execution.max_concurrent_executions + 1,  # Fetch just enough to check limit
        )

        running_count = 0
        for _ in self.repository.retrieve_by_pipeline_config_id(execution.pipeline_config_id, criteria):
            running_count += 1
            if running_count >= execution.max_concurrent_executions:
                return True

        return False

    def _start(
        self,
        execution: Workflow,
        message: StartWorkflow,
    ) -> None:
        """Start the execution."""
        initial_stages = execution.initial_stages()

        if not initial_stages:
            logger.warning("No initial stages found for execution %s", execution.id)
            self.set_workflow_status(execution, WorkflowStatus.TERMINAL)
            # Atomic: update execution status + message deduplication
            with self.repository.transaction(self.queue) as txn:
                txn.update_workflow_status(execution)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="StartWorkflow",
                        execution_id=message.execution_id,
                    )
            # Publish ExecutionComplete event
            return

        # Mark as running
        self.set_workflow_status(execution, WorkflowStatus.RUNNING)
        execution.start_time = self.current_time_millis()

        # Atomic: update execution status + queue all initial stages + message deduplication
        with self.repository.transaction(self.queue) as txn:
            txn.update_workflow_status(execution)
            if message.message_id:
                txn.mark_message_processed(
                    message_id=message.message_id,
                    handler_type="StartWorkflow",
                    execution_id=message.execution_id,
                )
            for stage in initial_stages:
                logger.debug(
                    "Queuing initial stage %s (%s) for execution %s",
                    stage.name,
                    stage.id,
                    execution.id,
                )
                txn.push_message(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=stage.id,
                    )
                )

        # Audit log
        audit(
            event_type="WORKFLOW_LIFECYCLE",
            action="START",
            user=execution.trigger.user or "system",
            resource_type="workflow",
            resource_id=execution.id,
            details={
                "application": execution.application,
                "name": execution.name,
                "initial_stages": len(initial_stages),
            },
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
