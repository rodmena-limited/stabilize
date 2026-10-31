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
from stabilize.handlers.admission import limits_concurrency, start_running, try_lock_admission
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelWorkflow,
    StartWaitingWorkflows,
    StartWorkflow,
)
from stabilize.queue.ownership import queue_table_name
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import StoreTransaction, WorkflowStore
    from stabilize.queue import Queue

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
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder)

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
                self._terminate(execution, message)
                return

            # Check if start time has expired
            if self._is_after_start_time_expiry(execution):
                logger.warning("Execution %s start time expired, canceling", execution.id)
                self.push_marked(
                    message,
                    "StartWorkflow",
                    CancelWorkflow(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        user="system",
                        reason="Could not begin execution before start time expiry",
                    ),
                )
                return

            execution.queue_table = queue_table_name(self.queue)
            if limits_concurrency(execution):
                self._admit(execution, message)
                return

            with self.repository.transaction(self.queue) as txn:
                started = self._start_in(txn, execution, message)
            self._after_start(execution, started)

        self.with_execution(message, on_execution)

    def _admit(self, execution: Workflow, message: StartWorkflow) -> None:
        config_id = execution.pipeline_config_id or ""
        with self.repository.transaction(self.queue) as txn:
            if try_lock_admission(txn, config_id):
                started = self._admit_locked(txn, execution, message, config_id)
            else:
                started = self._start_in(txn, execution, message)
        self._after_start(execution, started)

    def _admit_locked(
        self, txn: StoreTransaction, execution: Workflow, message: StartWorkflow, config_id: str
    ) -> list[StageExecution] | None:
        if txn.workflow_status(execution.id)[0] != WorkflowStatus.NOT_STARTED.name:
            self._mark(txn, message)
            return None
        limit = execution.max_concurrent_executions
        running = txn.count_running(config_id)
        waiting_older = [w for w in txn.buffered_ids(config_id) if w < execution.id]
        if waiting_older and running < limit:
            txn.push_message(StartWaitingWorkflows(pipeline_config_id=config_id, purge_queue=False))
        if running >= limit or waiting_older:
            logger.info(
                "Execution %s buffered (running=%d limit=%d older waiting=%d)",
                execution.id,
                running,
                limit,
                len(waiting_older),
            )
            self.set_workflow_status(execution, WorkflowStatus.BUFFERED)
            txn.update_workflow_status(execution)
            self._mark(txn, message)
            return None
        return self._start_in(txn, execution, message)

    def _mark(self, txn: StoreTransaction, message: StartWorkflow) -> None:
        if message.message_id:
            txn.mark_message_processed(
                message_id=message.message_id,
                handler_type="StartWorkflow",
                execution_id=message.execution_id,
            )

    def _start_in(
        self, txn: StoreTransaction, execution: Workflow, message: StartWorkflow
    ) -> list[StageExecution] | None:
        initial_stages = execution.initial_stages()
        if not initial_stages:
            logger.warning("No initial stages found for execution %s", execution.id)
            self.set_workflow_status(execution, WorkflowStatus.TERMINAL)
            txn.update_workflow_status(execution)
            self._mark(txn, message)
            return None
        start_running(self, txn, execution, initial_stages)
        self._mark(txn, message)
        return initial_stages

    def _after_start(self, execution: Workflow, initial_stages: list[StageExecution] | None) -> None:
        if initial_stages is None:
            return
        if self.event_recorder:
            self.set_event_context(execution.id)
            self.event_recorder.record_workflow_created(execution, source_handler="StartWorkflowHandler")
            self.event_recorder.record_workflow_started(
                execution,
                initial_stage_ids=[s.id for s in initial_stages],
                source_handler="StartWorkflowHandler",
            )
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
        logger.info("Started execution %s with %d initial stage(s)", execution.id, len(initial_stages))

    def _terminate(self, execution: Workflow, message: StartWorkflow) -> None:
        self.set_workflow_status(execution, WorkflowStatus.CANCELED)
        execution.end_time = self.current_time_millis()
        with self.repository.transaction(self.queue) as txn:
            txn.update_workflow_status(execution)
            self._mark(txn, message)

    def _is_after_start_time_expiry(self, execution: Workflow) -> bool:
        """Check if current time is past start time expiry."""
        if execution.start_time_expiry is None:
            return False
        return self.current_time_millis() > execution.start_time_expiry
