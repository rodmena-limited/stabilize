"""
SkipStageHandler - handles stage skipping.

This handler is invoked when a stage should be skipped due to conditional
execution or start time expiry.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from stabilize.dag.readiness import PRUNED
from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.cancel_hooks import run_cancel_hooks
from stabilize.handlers.deferred_choice import record_settle_row
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteWorkflow,
    ContinueParentStage,
    SkipStage,
    StartStage,
)
from stabilize.resilience.config import HandlerConfig
from stabilize.tasks.handles import bind_engine

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.tasks.registry import TaskRegistry

logger = logging.getLogger(__name__)

_PRUNABLE_LIVE_STATUSES = frozenset({WorkflowStatus.RUNNING, WorkflowStatus.SUSPENDED, WorkflowStatus.PAUSED})
_LIVE_TASK_STATUSES = frozenset(
    {WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING, WorkflowStatus.SUSPENDED, WorkflowStatus.PAUSED}
)


class SkipStageHandler(StabilizeHandler[SkipStage]):
    """
    Handler for SkipStage messages.

    Execution flow:
    1. Check if stage is still in a skippable state
    2. Set stage status to SKIPPED
    3. Set end time
    4. Atomically store the stage and trigger downstream stages
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
        task_registry: TaskRegistry | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)
        self.task_registry = task_registry

    @property
    def message_type(self) -> type[SkipStage]:
        return SkipStage

    def handle(self, message: SkipStage) -> None:
        """Handle the SkipStage message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings; a pruned stage's task hooks run once, after the commit.
        """
        settled: dict[str, Any] = {}
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message, settled),
            f"skipping stage {message.stage_id}",
        )
        stage = settled.get("stage")
        if stage is not None and message.pruned:
            with bind_engine(self.repository, self.queue):
                run_cancel_hooks(self.task_registry, stage, settled["moved"])
                self.run_stage_finalizers(stage)

    def _handle_with_retry(self, message: SkipStage, settled: dict[str, Any]) -> None:
        """Inner handle logic to be retried."""
        settled.clear()

        def on_stage(stage: StageExecution) -> None:
            live = stage.status in _PRUNABLE_LIVE_STATUSES
            if stage.status != WorkflowStatus.NOT_STARTED and not (message.pruned and live):
                logger.debug(
                    "Ignoring SkipStage for %s (%s) - already %s",
                    stage.name,
                    stage.id,
                    stage.status,
                )
                return

            moved: list[TaskExecution] = []
            if message.pruned:
                from stabilize.resilience.cancellation import cancel_task

                stage.context[PRUNED] = True
                for task in stage.tasks:
                    if task.status in _LIVE_TASK_STATUSES:
                        if task.status != WorkflowStatus.NOT_STARTED:
                            moved.append(task)
                        self.set_task_status(task, WorkflowStatus.CANCELED)
                        task.end_time = self.current_time_millis()
                        cancel_task(task.id)
            self.set_stage_status(stage, WorkflowStatus.SKIPPED)
            stage.end_time = self.current_time_millis()
            settled["stage"] = stage
            settled["moved"] = moved

            logger.info("Skipped stage %s (%s)%s", stage.name, stage.id, " (pruned)" if message.pruned else "")

            if self.event_recorder:
                self.set_event_context(stage.execution.id if stage.execution else "")
                self.event_recorder.record_stage_skipped(
                    stage,
                    reason="Stage pruned" if message.pruned else "Stage skipped",
                    source_handler="SkipStageHandler",
                )

            # Get downstream stages and parent info BEFORE transaction
            execution = stage.execution
            downstream_stages = self.repository.get_downstream_stages(execution.id, stage.ref_id)
            phase = stage.synthetic_stage_owner

            # Atomic: store stage + push all downstream/parent messages together
            live_children = [s for s in stage.synthetic_stages() if not s.status.is_complete] if message.pruned else []
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                record_settle_row(txn, execution.id, stage)
                for child in live_children:
                    txn.push_message(
                        SkipStage(
                            execution_type=execution.type.value,
                            execution_id=execution.id,
                            stage_id=child.id,
                            pruned=True,
                        )
                    )

                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="SkipStage",
                        execution_id=message.execution_id,
                    )

                if downstream_stages:
                    # Start all downstream stages
                    for downstream in downstream_stages:
                        txn.push_message(
                            StartStage(
                                execution_type=execution.type.value,
                                execution_id=execution.id,
                                stage_id=downstream.id,
                            )
                        )
                elif phase is not None:
                    # Synthetic stage - notify parent
                    parent_id = stage.parent_stage_id
                    if parent_id:
                        txn.push_message(
                            ContinueParentStage(
                                execution_type=execution.type.value,
                                execution_id=execution.id,
                                stage_id=parent_id,
                                phase=phase,
                            )
                        )
                else:
                    # Terminal stage - complete workflow
                    txn.push_message(
                        CompleteWorkflow(
                            execution_type=execution.type.value,
                            execution_id=execution.id,
                        )
                    )

        self.with_stage(message, on_stage)
