"""
StartWaitingWorkflowsHandler - starts buffered executions.

This handler is triggered when a pipeline execution completes.
It checks if there are any BUFFERED executions for the same pipeline config
and starts them if capacity allows.
"""

from __future__ import annotations

import logging

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowCriteria
from stabilize.queue.messages import (
    StartWaitingWorkflows,
    StartWorkflow,
)

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

    @property
    def message_type(self) -> type[StartWaitingWorkflows]:
        return StartWaitingWorkflows

    def handle(self, message: StartWaitingWorkflows) -> None:
        """Handle the StartWaitingWorkflows message."""
        if not message.pipeline_config_id:
            return

        # Get buffered executions (oldest first)
        criteria = WorkflowCriteria(
            statuses={WorkflowStatus.BUFFERED},
            page_size=100,  # Arbitrary batch size
        )
        # Note: repository retrieve usually orders by start_time DESC.
        # We want oldest first (FIFO). Ideally we'd sort by created_at ASC.
        # The default implementation sorts by start_time DESC.
        # Start time for buffered is NOT_STARTED time? Or created_at?
        # store.retrieve... returns executions.
        # We will fetch and sort in memory for now.

        buffered = list(self.repository.retrieve_by_pipeline_config_id(message.pipeline_config_id, criteria))

        # Sort by creation time (assuming id is ULID/monotonic or created_at)
        # ULID is monotonic.
        buffered.sort(key=lambda w: w.id)

        if not buffered:
            return

        # Check running count
        running_criteria = WorkflowCriteria(statuses={WorkflowStatus.RUNNING})
        running = list(self.repository.retrieve_by_pipeline_config_id(message.pipeline_config_id, running_criteria))
        running_count = len(running)

        # We assume all buffered have same config limit (from pipeline config)
        # Use limit from first buffered execution
        limit = buffered[0].max_concurrent_executions

        if limit <= 0:
            # No limit? Just start all?
            # If they were buffered, they presumably had a limit.
            # But if config changed, maybe now 0.
            # Safety: start one by one.
            available_slots = len(buffered)
        else:
            available_slots = max(0, limit - running_count)

        logger.info(
            "Processing waiting workflows for %s: %d buffered, %d running, %d limit, %d slots",
            message.pipeline_config_id,
            len(buffered),
            running_count,
            limit,
            available_slots,
        )

        for i, execution in enumerate(buffered):
            if i < available_slots:
                # Promote to NOT_STARTED so StartWorkflowHandler picks it up
                # (actually we push StartWorkflow message directly)

                # Wait, StartWorkflowHandler checks limit AGAIN.
                # If we push StartWorkflow, it will check running count.
                # If we do it concurrently, it might race.
                # But queue processing is sequential per handler usually?
                # No, parallel workers.

                # Better approach:
                # Mark execution as NOT_STARTED in DB.
                # Then push StartWorkflow.

                execution.status = WorkflowStatus.NOT_STARTED
                self.repository.update_status(execution)

                logger.info("Promoting buffered execution %s to queued", execution.id)
                self.queue.push(
                    StartWorkflow(
                        execution_type=execution.type.value,
                        execution_id=execution.id,
                    )
                )
            elif message.purge_queue:
                # Cancel remaining
                logger.info("Purging waiting execution %s", execution.id)
                self.repository.cancel(
                    execution.id,
                    canceled_by="system",
                    reason="Queue purged by keepWaitingPipelines=False policy",
                )
            else:
                # Leave in buffer
                pass
