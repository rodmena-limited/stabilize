"""
RunTaskHandler - executes tasks.

This is the handler that actually runs task implementations.
It handles execution, retries, timeouts, and result processing.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteTask,
    PauseTask,
    RunTask,
)
from stabilize.tasks.interface import RetryableTask, Task
from stabilize.tasks.registry import TaskNotFoundError, TaskRegistry
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue

logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    """Raised when a task times out."""

    pass


class RunTaskHandler(StabilizeHandler[RunTask]):
    """
    Handler for RunTask messages.

    This is where tasks are actually executed. The handler:
    1. Resolves the task implementation
    2. Checks for cancellation/pause
    3. Checks for timeout
    4. Executes the task
    5. Processes the result
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        task_registry: TaskRegistry,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        super().__init__(queue, repository, retry_delay)
        self.task_registry = task_registry

    @property
    def message_type(self) -> type[RunTask]:
        return RunTask

    def handle(self, message: RunTask) -> None:
        """Handle the RunTask message."""

        def on_task(stage: StageExecution, task_model: TaskExecution) -> None:
            execution = stage.execution

            # Resolve task implementation
            try:
                task = self._resolve_task(message.task_type, task_model)
            except TaskNotFoundError as e:
                logger.error("Task type not found: %s", message.task_type)
                self._complete_with_error(stage, task_model, message, str(e))
                return

            # Check execution state
            if execution.is_canceled:
                self._handle_cancellation(stage, task_model, task, message)
                return

            if execution.status.is_complete:
                self.queue.push(
                    CompleteTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        status=WorkflowStatus.CANCELED,
                    )
                )
                return

            if execution.status == WorkflowStatus.PAUSED:
                self.queue.push(
                    PauseTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                    )
                )
                return

            # Check for manual skip
            if stage.context.get("manualSkip"):
                self.queue.push(
                    CompleteTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        status=WorkflowStatus.SKIPPED,
                    )
                )
                return

            # Check for timeout
            try:
                self._check_for_timeout(stage, task_model, task)
            except TimeoutError as e:
                logger.info("Task %s timed out: %s", task_model.name, e)
                result = task.on_timeout(stage) if hasattr(task, "on_timeout") else None
                if result is None:
                    self._complete_with_error(stage, task_model, message, str(e))
                    return
                else:
                    self._process_result(stage, task_model, result, message)
                    return

            # Execute the task
            try:
                result = task.execute(stage)
                self._process_result(stage, task_model, result, message)
            except Exception as e:
                logger.error(
                    "Error executing task %s: %s",
                    task_model.name,
                    e,
                    exc_info=True,
                )
                self._handle_exception(stage, task_model, task, message, e)

        self.with_task(message, on_task)

    def _resolve_task(
        self,
        task_type: str,
        task_model: TaskExecution,
    ) -> Task:
        """Resolve the task implementation."""
        # Try by class name first
        try:
            return self.task_registry.get_by_class(task_model.implementing_class)
        except TaskNotFoundError:
            pass

        # Try by type name
        return self.task_registry.get(task_type)

    def _check_for_timeout(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        task: Task,
    ) -> None:
        """Check if task has timed out."""
        if not isinstance(task, RetryableTask):
            return

        if task_model.start_time is None:
            return

        start_time = task_model.start_time
        current_time = self.current_time_millis()
        elapsed = timedelta(milliseconds=current_time - start_time)

        # Get timeout (potentially dynamic)
        timeout = task.get_dynamic_timeout(stage)

        # Account for paused time
        paused_duration = timedelta(milliseconds=stage.execution.paused_duration_relative_to(start_time))
        actual_elapsed = elapsed - paused_duration

        if actual_elapsed > timeout:
            raise TimeoutError(f"Task timed out after {actual_elapsed} (timeout: {timeout})")

    def _process_result(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        result: TaskResult,
        message: RunTask,
    ) -> None:
        """Process a task result.

        Uses atomic transactions to ensure stage updates and message pushes
        are committed together. This prevents orphaned workflows where the
        stage is saved but the continuation message is lost.
        """
        # Store outputs in stage
        if result.context:
            stage.context.update(result.context)
        if result.outputs:
            stage.outputs.update(result.outputs)

        # Handle based on status - use atomic transactions
        if result.status == WorkflowStatus.RUNNING:
            # Task needs to be re-executed
            delay = self._get_backoff_period(stage, task_model, message)

            # Atomic: store stage + push message together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                txn.push_message(message, int(delay.total_seconds()))

            logger.debug(
                "Task %s still running, re-queuing with %s delay",
                task_model.name,
                delay,
            )

        elif result.status in {
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.REDIRECT,
            WorkflowStatus.SKIPPED,
            WorkflowStatus.FAILED_CONTINUE,
            WorkflowStatus.STOPPED,
        }:
            # Atomic: store stage + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                txn.push_message(
                    CompleteTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        status=result.status,
                    )
                )

        elif result.status == WorkflowStatus.CANCELED:
            status = stage.failure_status(default=result.status)

            # Atomic: store stage + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                txn.push_message(
                    CompleteTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        status=status,
                        original_status=result.status,
                    )
                )

        elif result.status == WorkflowStatus.TERMINAL:
            status = stage.failure_status(default=result.status)

            # Atomic: store stage + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                txn.push_message(
                    CompleteTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        status=status,
                        original_status=result.status,
                    )
                )

        else:
            logger.warning("Unhandled task status: %s", result.status)
            self.repository.store_stage(stage)

    def _get_backoff_period(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        message: RunTask,
    ) -> timedelta:
        """Calculate backoff period for retry."""
        # Try to get the task and use its backoff
        try:
            task = self._resolve_task(message.task_type, task_model)
            if isinstance(task, RetryableTask):
                elapsed = timedelta(milliseconds=self.current_time_millis() - (task_model.start_time or 0))
                return task.get_dynamic_backoff_period(stage, elapsed)
        except TaskNotFoundError:
            pass

        # Default backoff
        return timedelta(seconds=1)

    def _handle_cancellation(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        task: Task,
        message: RunTask,
    ) -> None:
        """Handle execution cancellation.

        Uses atomic transaction for stage update + message push.
        """
        result = task.on_cancel(stage) if hasattr(task, "on_cancel") else None
        if result:
            stage.context.update(result.context)
            stage.outputs.update(result.outputs)

        # Atomic: store stage (if modified) + push CompleteTask together
        with self.repository.transaction(self.queue) as txn:
            if result:
                txn.store_stage(stage)
            txn.push_message(
                CompleteTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    status=WorkflowStatus.CANCELED,
                )
            )

    def _handle_exception(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        task: Task,
        message: RunTask,
        exception: Exception,
    ) -> None:
        """Handle task execution exception.

        Uses atomic transaction for stage update + message push.
        """
        # TODO: Check if exception is retryable
        # For now, treat all exceptions as terminal

        exception_details = {
            "details": {
                "error": str(exception),
                "errors": [str(exception)],
            }
        }

        stage.context["exception"] = exception_details
        task_model.task_exception_details["exception"] = exception_details

        status = stage.failure_status(default=WorkflowStatus.TERMINAL)

        # Atomic: store stage + push CompleteTask together
        with self.repository.transaction(self.queue) as txn:
            txn.store_stage(stage)
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

    def _complete_with_error(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        message: RunTask,
        error: str,
    ) -> None:
        """Complete task with an error.

        Uses atomic transaction for stage update + message push.
        """
        stage.context["exception"] = {
            "details": {"error": error},
        }

        # Atomic: store stage + push CompleteTask together
        with self.repository.transaction(self.queue) as txn:
            txn.store_stage(stage)
            txn.push_message(
                CompleteTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    status=WorkflowStatus.TERMINAL,
                )
            )
