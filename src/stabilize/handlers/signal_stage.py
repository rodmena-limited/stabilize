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
from stabilize.handlers.signal_refusal import RefusalTracker
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
        self._refusals = RefusalTracker()

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
            # Attribute anything recorded while handling this signal to whoever
            # sent it, rather than to "system".
            self.set_event_context(message.execution_id, actor=message.user or None)

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
                    if self.event_recorder:
                        self.event_recorder.record_stage_resumed(
                            stage,
                            signal_name=message.signal_name,
                            source_handler="SignalStageHandler",
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
                if stage.status.is_complete:
                    self._refuse_undeliverable(message, stage)
                    return

                store_backed = self.repository.supports_signal_storage()
                context_buffered = stage.context.get("_buffered_signals", [])
                if store_backed:
                    depth = self.repository.pending_signal_count(
                        stage.execution.id, stage.ref_id
                    ) + len(context_buffered)
                else:
                    depth = len(context_buffered)

                if depth >= self.handler_config.signal_buffer_max:
                    self._refuse_overflow(message, stage, depth)
                    return

                logger.info(
                    "Buffering persistent signal '%s' for stage %s (current status: %s)",
                    message.signal_name,
                    stage.name,
                    stage.status,
                )

                if store_backed:
                    # workflow_signals, not stage context: the context copy grew
                    # unbounded and is visible to every consumer reading that
                    # column. Rows already carrying _buffered_signals keep
                    # working -- the consume path reads both.
                    self.repository.buffer_signal(
                        stage.execution.id,
                        stage.ref_id,
                        message.signal_name,
                        message.signal_data,
                    )
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
                else:
                    context_buffered.append(
                        {
                            "signal_name": message.signal_name,
                            "signal_data": message.signal_data,
                        }
                    )
                    stage.context["_buffered_signals"] = context_buffered
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

    def _mark_processed(self, message: SignalStage) -> None:
        if not message.message_id:
            return
        with self.repository.transaction(self.queue) as txn:
            txn.mark_message_processed(
                message_id=message.message_id,
                handler_type="SignalStage",
                execution_id=message.execution_id,
            )

    def _refuse_undeliverable(self, message: SignalStage, stage: StageExecution) -> None:
        count, emit = self._refusals.record(message.execution_id, stage.ref_id)
        log = logger.warning if emit else logger.debug
        log(
            "Refused %d persistent signal(s) for stage %s (ref_id=%s, execution=%s): "
            "stage status %s is complete and can never consume them; "
            "most recent signal_name=%s",
            count,
            stage.name,
            stage.ref_id,
            message.execution_id,
            stage.status,
            message.signal_name,
        )
        self._mark_processed(message)

    def _refuse_overflow(self, message: SignalStage, stage: StageExecution, depth: int) -> None:
        count, emit = self._refusals.record(message.execution_id, stage.ref_id)
        log = logger.warning if emit else logger.debug
        log(
            "Refused %d persistent signal(s) for stage %s (ref_id=%s, execution=%s): "
            "buffer holds %d signals, at the STABILIZE_SIGNAL_BUFFER_MAX limit of %d; "
            "most recent signal_name=%s, dead-lettered",
            count,
            stage.name,
            stage.ref_id,
            message.execution_id,
            depth,
            self.handler_config.signal_buffer_max,
            message.signal_name,
        )
        dlq = getattr(self.queue, "move_to_dlq", None)
        if dlq is not None and message.message_id:
            try:
                dlq(
                    message.message_id,
                    error=(
                        f"signal_buffer_full: stage {stage.ref_id} holds {depth} buffered "
                        f"signals (limit {self.handler_config.signal_buffer_max})"
                    ),
                )
            except Exception:
                logger.exception(
                    "Failed to dead-letter overflowed signal '%s' for stage %s",
                    message.signal_name,
                    stage.ref_id,
                )
        self._mark_processed(message)
