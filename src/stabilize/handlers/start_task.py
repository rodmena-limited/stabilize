"""
StartTaskHandler - handles task startup.

This handler prepares a task for execution and triggers RunTask.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.error_codes import classify_error
from stabilize.errors import truncate_error
from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.run_task.execution import resolve_task
from stabilize.models.stage.completion_records import mark_gate_checked, record_completion
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteTask,
    RunTask,
    StartTask,
)
from stabilize.resilience.config import HandlerConfig
from stabilize.tasks.interface import SkippableTask
from stabilize.tasks.registry import TaskNotFoundError, TaskRegistry

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class StartTaskHandler(StabilizeHandler[StartTask]):
    """
    Handler for StartTask messages.

    Execution flow:
    1. Check if task is enabled (SkippableTask)
       - If not: Push CompleteTask(SKIPPED)
    2. Set task status to RUNNING
    3. Set task start time
    4. Push RunTask
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        task_registry: TaskRegistry,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)
        self.task_registry = task_registry

    @property
    def message_type(self) -> type[StartTask]:
        return StartTask

    def handle(self, message: StartTask) -> None:
        """Handle the StartTask message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"starting task {message.task_id}",
        )

    def _handle_with_retry(self, message: StartTask) -> None:
        """Inner handle logic to be retried."""

        def on_task(stage: StageExecution, task_model: TaskExecution) -> None:
            # Idempotency check - only start tasks that are NOT_STARTED
            if task_model.status != WorkflowStatus.NOT_STARTED:
                logger.debug(
                    "Ignoring StartTask for %s (%s) - already %s",
                    task_model.name,
                    task_model.id,
                    task_model.status,
                )
                # Mark message as processed to prevent infinite reprocessing
                if message.message_id:
                    with self.repository.transaction(self.queue) as txn:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="StartTask",
                            execution_id=message.execution_id,
                        )
                return

            try:
                task_impl = resolve_task(task_model.implementing_class, task_model, self.task_registry)
            except TaskNotFoundError:
                task_impl = None
            if isinstance(task_impl, SkippableTask):
                try:
                    enabled = task_impl.is_enabled(stage)
                except Exception as gate_error:
                    self._fail_on_gate_error(stage, task_model, message, gate_error)
                    return
                if not enabled:
                    logger.info("Skipping task %s (disabled)", task_model.name)
                    self.set_task_status(task_model, WorkflowStatus.SKIPPED)
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
                        if message.message_id:
                            txn.mark_message_processed(
                                message_id=message.message_id,
                                handler_type="StartTask",
                                execution_id=message.execution_id,
                            )
                        txn.push_message(
                            CompleteTask(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                                task_id=message.task_id,
                                status=WorkflowStatus.SKIPPED,
                            )
                        )
                    return
                mark_gate_checked(stage, task_model.id)

            # Update task status
            self.set_task_status(task_model, WorkflowStatus.RUNNING)
            task_model.start_time = self.current_time_millis()

            # Atomic: store stage + push RunTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="StartTask",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    RunTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        task_type=task_model.implementing_class,
                    )
                )

            if self.event_recorder:
                self.set_event_context(stage.execution.id)
                self.event_recorder.record_task_started(
                    task_model, stage.execution.id, source_handler="StartTaskHandler"
                )

            logger.debug(
                "Started task %s (%s) in stage %s",
                task_model.name,
                task_model.id,
                stage.name,
            )

        self.with_task(message, on_task)

    def _fail_on_gate_error(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        message: StartTask,
        error: Exception,
    ) -> None:
        logger.error("is_enabled raised for task %s: %s", task_model.name, error)
        text = truncate_error(f"is_enabled raised {type(error).__name__}: {error}")
        details = {"details": {"error": text, "errors": [text], "error_code": classify_error(error).value}}
        stage.context["exception"] = details
        task_model.task_exception_details["exception"] = details
        self.set_task_status(task_model, WorkflowStatus.RUNNING)
        task_model.start_time = self.current_time_millis()
        status = stage.failure_status(default=WorkflowStatus.TERMINAL)
        record_completion(stage, task_model.id, status, WorkflowStatus.TERMINAL)
        with self.repository.transaction(self.queue) as txn:
            txn.store_stage(stage)
            if message.message_id:
                txn.mark_message_processed(
                    message_id=message.message_id,
                    handler_type="StartTask",
                    execution_id=message.execution_id,
                )
            txn.push_message(
                CompleteTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    status=status,
                    original_status=WorkflowStatus.TERMINAL,
                )
            )
