"""
CompleteTaskHandler - handles task completion.

This handler updates task status and triggers either the next task
or stage completion.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.stage.completion_records import clear_completion
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteStage,
    CompleteTask,
    StartTask,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)

_CONTINUING_STATUSES = frozenset({WorkflowStatus.SUCCEEDED, WorkflowStatus.SKIPPED})
_TASK_FAILED_CONTINUE = frozenset({None, WorkflowStatus.FAILED_CONTINUE})


def continues_in_stage(message: CompleteTask) -> bool:
    """Whether the stage goes on to its next task after this completion.

    True for SUCCEEDED and SKIPPED, and for FAILED_CONTINUE returned by the
    task itself. Every other status, including a FAILED_CONTINUE converted from
    another status by continuePipelineOnFailure, ends the stage.
    """
    if message.status in _CONTINUING_STATUSES:
        return True
    return message.status == WorkflowStatus.FAILED_CONTINUE and message.original_status in _TASK_FAILED_CONTINUE


def accepts_start_task_skip(task: TaskExecution, message: CompleteTask) -> bool:
    """A CompleteTask(SKIPPED) for a task StartTask skipped: SKIPPED with no end_time yet."""
    return message.status == WorkflowStatus.SKIPPED and task.status == WorkflowStatus.SKIPPED and task.end_time is None


class CompleteTaskHandler(StabilizeHandler[CompleteTask]):
    """
    Handler for CompleteTask messages.

    Execution flow:
    1. Update task status and end time
    2. If there's a next task: Push StartTask
    3. Otherwise: Push CompleteStage
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
    def message_type(self) -> type[CompleteTask]:
        return CompleteTask

    def handle(self, message: CompleteTask) -> None:
        """Handle the CompleteTask message.

        Uses atomic transactions to ensure stage updates and message pushes
        are committed together, preventing orphaned states.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"completing task {message.task_id}",
        )

    def _handle_with_retry(self, message: CompleteTask) -> None:
        """Inner handle logic to be retried."""

        def on_task(stage: StageExecution, task: TaskExecution) -> None:
            # Idempotency check - only complete tasks that are RUNNING
            stale = message.task_start_time is not None and message.task_start_time != task.start_time
            if stale or (task.status != WorkflowStatus.RUNNING and not accepts_start_task_skip(task, message)):
                logger.debug(
                    "Ignoring CompleteTask for %s (%s) - already %s",
                    task.name,
                    task.id,
                    task.status,
                )
                # Mark message as processed to prevent infinite reprocessing
                if message.message_id:
                    with self.repository.transaction(self.queue) as txn:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="CompleteTask",
                            execution_id=message.execution_id,
                        )
                return

            # Update task status
            self.set_task_status(task, message.status)
            task.end_time = self.current_time_millis()
            clear_completion(stage, task.id)

            logger.debug("Task %s completed with status %s", task.name, message.status)

            def record_completion_event() -> None:
                # Called INSIDE the store transaction: the event joins the
                # same commit as the task state (no phantom events) and its
                # bus publication is deferred until after commit.
                if not self.event_recorder:
                    return
                workflow_id = stage.execution.id if stage.execution else ""
                self.set_event_context(workflow_id)
                if message.status.is_failure:
                    error = "Task failed"
                    if task.task_exception_details:
                        error = task.task_exception_details.get("exception", str(task.task_exception_details))
                    self.event_recorder.record_task_failed(
                        task,
                        workflow_id=workflow_id,
                        error=error,
                        source_handler="CompleteTaskHandler",
                    )
                elif message.status == WorkflowStatus.SKIPPED:
                    pass  # Skipped tasks don't need completion events
                else:
                    self.event_recorder.record_task_completed(
                        task,
                        workflow_id=workflow_id,
                        outputs=stage.outputs,
                        source_handler="CompleteTaskHandler",
                    )

            # For REDIRECT status, the task initiated a jump to another stage.
            # JumpToStageHandler handles the flow control, so we just complete
            # the task and don't start the next task or complete the stage.
            if message.status == WorkflowStatus.REDIRECT:
                logger.debug(
                    "Task %s completed with REDIRECT - flow handled by JumpToStageHandler",
                    task.name,
                )
                # Atomic: store stage only (no next message)
                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    record_completion_event()
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="CompleteTask",
                            execution_id=message.execution_id,
                        )
                return

            next_task = stage.next_task(task) if continues_in_stage(message) else None
            if next_task is None:
                self._skip_tasks_after(stage, task)

            # Atomic: store stage + push next message together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                record_completion_event()

                # Atomic deduplication
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="CompleteTask",
                        execution_id=message.execution_id,
                    )

                if next_task is not None:
                    txn.push_message(
                        StartTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=next_task.id,
                        )
                    )
                else:
                    # No more tasks - complete stage
                    txn.push_message(
                        CompleteStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                        )
                    )

        self.with_task(message, on_task)

    def _skip_tasks_after(self, stage: StageExecution, task: TaskExecution) -> None:
        try:
            index = stage.tasks.index(task)
        except ValueError:
            return
        now = self.current_time_millis()
        for later in stage.tasks[index + 1 :]:
            if later.status == WorkflowStatus.NOT_STARTED:
                self.set_task_status(later, WorkflowStatus.SKIPPED)
                later.end_time = now
