"""
CancelStageHandler - handles stage cancellation.

This handler is invoked when a stage needs to be canceled, typically
due to upstream failure or execution cancellation.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.cancel_hooks import run_cancel_hooks
from stabilize.handlers.deferred_choice import record_settle_row
from stabilize.models.stage.completion_records import recorded_completion
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CancelStage, CompleteStage, CompleteWorkflow
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.tasks.registry import TaskRegistry

logger = logging.getLogger(__name__)

_LIVE_TASK_STATUSES = frozenset(
    {WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING, WorkflowStatus.SUSPENDED, WorkflowStatus.PAUSED}
)


class CancelStageHandler(StabilizeHandler[CancelStage]):
    """
    Handler for CancelStage messages.

    Execution flow:
    1. Check if stage is still in a cancellable state
    2. Cancel the stage's NOT_STARTED and RUNNING tasks and the stage
    3. In one transaction: store the stage and push CompleteWorkflow (top-level
       stage) or CompleteStage for the parent (synthetic stage)
    4. After the commit: on_cancel for each task this handler moved from
       RUNNING (unless RunTask already canceled it), on_cleanup for every task,
       and the stage's finalizers
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
    def message_type(self) -> type[CancelStage]:
        return CancelStage

    def handle(self, message: CancelStage) -> None:
        """Handle the CancelStage message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings; task hooks run once, after the commit.
        """
        settled: dict[str, Any] = {}
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message, settled),
            f"canceling stage {message.stage_id}",
        )
        stage = settled.get("stage")
        if stage is not None:
            self._run_hooks(stage, settled["moved"])
            self.run_stage_finalizers(stage)

    def _handle_with_retry(self, message: CancelStage, settled: dict[str, Any]) -> None:
        """Inner handle logic to be retried."""
        settled.clear()

        def on_stage(stage: StageExecution) -> None:
            if stage.status.is_complete:
                logger.debug(
                    "Ignoring CancelStage for %s (%s) - already %s",
                    stage.name,
                    stage.id,
                    stage.status,
                )
                return

            from stabilize.resilience.cancellation import cancel_task

            moved: list[TaskExecution] = []
            for task in stage.tasks:
                if task.status == WorkflowStatus.SKIPPED and task.end_time is None:
                    task.end_time = self.current_time_millis()
                if task.status in _LIVE_TASK_STATUSES:
                    if task.status != WorkflowStatus.NOT_STARTED and not (
                        task.status == WorkflowStatus.RUNNING and _canceled_by_run_task(stage, task)
                    ):
                        moved.append(task)
                    self.set_task_status(task, WorkflowStatus.CANCELED)
                    task.end_time = self.current_time_millis()
                    cancel_task(task.id)

            self.set_stage_status(stage, WorkflowStatus.CANCELED)
            stage.end_time = self.current_time_millis()

            live_children = [s for s in stage.synthetic_stages() if not s.status.is_complete]
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                record_settle_row(txn, message.execution_id, stage)
                for child in live_children:
                    txn.push_message(
                        CancelStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=child.id,
                        )
                    )
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="CancelStage",
                        execution_id=message.execution_id,
                    )
                if stage.synthetic_stage_owner is not None and stage.parent_stage_id is not None:
                    txn.push_message(
                        CompleteStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=stage.parent_stage_id,
                        )
                    )
                else:
                    txn.push_message(
                        CompleteWorkflow(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                        )
                    )
            settled["stage"] = stage
            settled["moved"] = moved

            if self.event_recorder:
                self.set_event_context(stage.execution.id if stage.execution else "")
                self.event_recorder.record_stage_canceled(stage, source_handler="CancelStageHandler")

            logger.info("Canceled stage %s (%s)", stage.name, stage.id)

        self.with_stage(message, on_stage)

    def _run_hooks(self, stage: StageExecution, moved: list[TaskExecution]) -> None:
        run_cancel_hooks(self.task_registry, stage, moved)


def _canceled_by_run_task(stage: StageExecution, task: TaskExecution) -> bool:
    recorded = recorded_completion(stage, task)
    return recorded is not None and recorded[0] == WorkflowStatus.CANCELED
