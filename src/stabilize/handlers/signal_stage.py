"""
SignalStageHandler - handles external signals for suspended stages.

Implements:
- WCP-23: Transient Trigger - signal is lost if stage not SUSPENDED
- WCP-24: Persistent Trigger - signal is buffered if stage not ready
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import RunTask, SignalStage, StartStage
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class SignalStageHandler(StabilizeHandler[SignalStage]):
    """
    Handler for SignalStage messages.

    Execution flow:
    1. Check if stage is SUSPENDED
       - If SUSPENDED: transition to RUNNING, merge signal data, push StartStage
       - If not SUSPENDED and transient: discard signal (WCP-23)
       - If not SUSPENDED and persistent: buffer signal for later (WCP-24)
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
    def message_type(self) -> type[SignalStage]:
        return SignalStage

    def handle(self, message: SignalStage) -> None:
        """Handle the SignalStage message."""
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"signaling stage {message.stage_id}",
        )

    def _handle_with_retry(self, message: SignalStage) -> None:
        """Inner handle logic to be retried."""

        def on_stage(stage: StageExecution) -> None:
            if stage.status == WorkflowStatus.SUSPENDED:
                # Stage is waiting for a signal - deliver it
                logger.info(
                    "Delivering signal '%s' to suspended stage %s",
                    message.signal_name,
                    stage.name,
                )

                # Merge signal data into stage context
                stage.context["_signal_name"] = message.signal_name
                stage.context["_signal_data"] = message.signal_data

                # Transition back to RUNNING
                self.set_stage_status(stage, WorkflowStatus.RUNNING)

                # Find the suspended task and set it back to RUNNING
                suspended_task = None
                for task in stage.tasks:
                    if task.status == WorkflowStatus.SUSPENDED:
                        suspended_task = task
                        task.status = WorkflowStatus.RUNNING
                        break

                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="SignalStage",
                            execution_id=message.execution_id,
                        )
                    if suspended_task:
                        # Push RunTask to re-execute the suspended task
                        txn.push_message(
                            RunTask(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                                task_id=suspended_task.id,
                            )
                        )
                    else:
                        # No suspended task found - re-start the stage
                        txn.push_message(
                            StartStage(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                            )
                        )
                return

            # Stage is not SUSPENDED
            if message.persistent:
                # WCP-24: Buffer the signal for later consumption
                logger.info(
                    "Buffering persistent signal '%s' for stage %s (current status: %s)",
                    message.signal_name,
                    stage.name,
                    stage.status,
                )
                # Store signal in stage context buffer
                buffered = stage.context.get("_buffered_signals", [])
                buffered.append(
                    {
                        "signal_name": message.signal_name,
                        "signal_data": message.signal_data,
                    }
                )
                stage.context["_buffered_signals"] = buffered

                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="SignalStage",
                            execution_id=message.execution_id,
                        )
            else:
                # WCP-23: Transient signal - discard
                logger.debug(
                    "Discarding transient signal '%s' for stage %s (not SUSPENDED, status: %s)",
                    message.signal_name,
                    stage.name,
                    stage.status,
                )
                if message.message_id:
                    with self.repository.transaction(self.queue) as txn:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="SignalStage",
                            execution_id=message.execution_id,
                        )

        self.with_stage(message, on_stage)
