"""
StartWaitingWorkflowsHandler - starts buffered executions.

This handler is triggered when a pipeline execution completes.
It checks if there are any BUFFERED executions for the same pipeline config
and starts them if capacity allows.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.audit import audit
from stabilize.handlers.admission import promote_waiting
from stabilize.handlers.base import StabilizeHandler
from stabilize.queue.messages import StartWaitingWorkflows
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class StartWaitingWorkflowsHandler(StabilizeHandler[StartWaitingWorkflows]):
    """
    Handler for StartWaitingWorkflows messages.

    Execution flow:
    1. Find buffered executions for the pipeline config
    2. Check concurrent limits
    3. Start executions up to the limit
    4. If purge_queue is True, cancel remaining buffered executions
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)

    @property
    def message_type(self) -> type[StartWaitingWorkflows]:
        return StartWaitingWorkflows

    def handle(self, message: StartWaitingWorkflows) -> None:
        """Handle the StartWaitingWorkflows message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"starting waiting workflows for {message.pipeline_config_id}",
        )

    def _handle_with_retry(self, message: StartWaitingWorkflows) -> None:
        """Inner handle logic to be retried."""
        if not message.pipeline_config_id:
            return
        started, canceled = promote_waiting(self, message.pipeline_config_id, message.purge_queue, message)
        for execution in canceled:
            self.repository.cancel(
                execution.id,
                canceled_by="system",
                reason="Queue purged by keepWaitingPipelines=False policy",
            )
        logger.info(
            "Waiting workflows for %s: %d started, %d canceled",
            message.pipeline_config_id,
            len(started),
            len(canceled),
        )
        for execution, initial in started:
            if self.event_recorder:
                self.set_event_context(execution.id)
                self.event_recorder.record_workflow_created(execution, source_handler="StartWaitingWorkflowsHandler")
                self.event_recorder.record_workflow_started(
                    execution,
                    initial_stage_ids=[s.id for s in initial],
                    source_handler="StartWaitingWorkflowsHandler",
                )
            audit(
                event_type="WORKFLOW_LIFECYCLE",
                action="START",
                user=execution.trigger.user or "system",
                resource_type="workflow",
                resource_id=execution.id,
                details={"application": execution.application, "name": execution.name, "initial_stages": len(initial)},
            )
