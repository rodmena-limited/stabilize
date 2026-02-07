"""
CancelRegionHandler - cancels all stages in a named region.

Implements WCP-25: Cancel Region.
Finds all stages with matching cancel_region and pushes CancelStage for each active one.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.queue.messages import CancelRegion, CancelStage
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class CancelRegionHandler(StabilizeHandler[CancelRegion]):
    """
    Handler for CancelRegion messages.

    Execution flow:
    1. Load the workflow execution
    2. Find all stages with matching cancel_region
    3. For each active stage, push CancelStage
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
    def message_type(self) -> type[CancelRegion]:
        return CancelRegion

    def handle(self, message: CancelRegion) -> None:
        """Handle the CancelRegion message."""

        def on_execution(execution: object) -> None:
            from stabilize.models.workflow import Workflow

            if not isinstance(execution, Workflow):
                return

            region = message.region
            if not region:
                logger.warning("CancelRegion message with empty region, ignoring")
                return

            # Find all stages in the region that are still active
            stages_to_cancel = [s for s in execution.stages if s.cancel_region == region and not s.status.is_complete]

            if not stages_to_cancel:
                logger.debug(
                    "No active stages found in cancel region '%s'",
                    region,
                )
                return

            logger.info(
                "Cancelling %d stages in region '%s'",
                len(stages_to_cancel),
                region,
            )

            with self.repository.transaction(self.queue) as txn:
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="CancelRegion",
                        execution_id=message.execution_id,
                    )
                for stage in stages_to_cancel:
                    txn.push_message(
                        CancelStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.id,
                        )
                    )

        self.with_execution(message, on_execution)
