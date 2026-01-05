"""
RunTaskHandler - executes tasks.

This is the handler that actually runs task implementations.
It handles execution, retries, timeouts, and result processing.
"""

from __future__ import annotations

import logging
import random
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.errors import TaskTimeoutError, is_transient
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

# Default timeout for tasks that don't specify one (5 minutes)
DEFAULT_TASK_TIMEOUT = timedelta(minutes=5)


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

            # Execute the task with timeout enforcement
            try:
                timeout = self._get_task_timeout(stage, task)
                result = self._execute_with_timeout(task, stage, timeout)
                self._process_result(stage, task_model, result, message)
            except TaskTimeoutError as e:
                logger.info("Task %s timed out: %s", task_model.name, e)
                timeout_result: TaskResult | None = task.on_timeout(stage) if hasattr(task, "on_timeout") else None
                if timeout_result is None:
                    self._complete_with_error(stage, task_model, message, str(e))
                else:
                    self._process_result(stage, task_model, timeout_result, message)
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

    def _get_task_timeout(
        self,
        stage: StageExecution,
        task: Task,
    ) -> timedelta:
        """Get the timeout for a task.

        Returns the dynamic timeout for RetryableTask or default timeout for others.
        """
        if isinstance(task, RetryableTask):
            return task.get_dynamic_timeout(stage)
        return DEFAULT_TASK_TIMEOUT

    def _execute_with_timeout(
        self,
        task: Task,
        stage: StageExecution,
        timeout: timedelta,
    ) -> TaskResult:
        """Execute a task with timeout enforcement.

        Runs the task in a separate thread and interrupts if it exceeds the timeout.

        Args:
            task: The task to execute
            stage: The stage execution context
            timeout: Maximum time allowed for execution

        Returns:
            The task result

        Raises:
            TaskTimeoutError: If the task exceeds the timeout
        """
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(task.execute, stage)
            try:
                return future.result(timeout=timeout.total_seconds())
            except FuturesTimeoutError:
                # Attempt to cancel the future (may not always succeed)
                future.cancel()
                raise TaskTimeoutError(
                    f"Task exceeded timeout of {timeout}",
                    task_name=getattr(task, "name", type(task).__name__),
                    stage_id=stage.id,
                    execution_id=stage.execution.id if stage.execution else None,
                )

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
        attempt: int = 1,
    ) -> timedelta:
        """Calculate backoff period for retry with exponential backoff and jitter.

        Uses exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at 60s.
        Adds ±25% jitter to prevent thundering herd.

        Args:
            stage: The stage execution context
            task_model: The task execution model
            message: The RunTask message
            attempt: The current attempt number (1-based)

        Returns:
            The calculated backoff period
        """
        # Try to get the task and use its backoff if it's a RetryableTask
        try:
            task = self._resolve_task(message.task_type, task_model)
            if isinstance(task, RetryableTask):
                elapsed = timedelta(milliseconds=self.current_time_millis() - (task_model.start_time or 0))
                return task.get_dynamic_backoff_period(stage, elapsed)
        except TaskNotFoundError:
            pass

        # Default exponential backoff with jitter
        # Exponential: 1s, 2s, 4s, 8s, 16s, 32s, capped at 60s
        base_delay = min(2 ** (attempt - 1), 60)

        # Add jitter (±25%) to prevent thundering herd
        jitter = base_delay * random.uniform(-0.25, 0.25)
        delay = base_delay + jitter

        return timedelta(seconds=max(1.0, delay))

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

        Uses is_transient() to determine if the error should be retried.
        Transient errors are rescheduled with exponential backoff.
        Permanent errors are marked as terminal.

        Uses atomic transaction for stage update + message push.
        """
        # Check if exception is retryable (transient)
        if is_transient(exception):
            logger.info(
                "Task %s encountered transient error, will retry: %s",
                task_model.name,
                exception,
            )

            # Get attempt count from message or default to 1
            attempt = getattr(message, "attempt", 1) or 1
            max_attempts = 10  # Maximum retry attempts

            if attempt < max_attempts:
                # Reschedule with backoff
                delay = self._get_backoff_period(stage, task_model, message, attempt + 1)

                # Create new message with incremented attempt
                retry_message = RunTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    task_type=message.task_type,
                )

                # Atomic: push retry message
                with self.repository.transaction(self.queue) as txn:
                    txn.push_message(retry_message, int(delay.total_seconds()))

                logger.debug(
                    "Task %s rescheduled for retry %d/%d with delay %s",
                    task_model.name,
                    attempt + 1,
                    max_attempts,
                    delay,
                )
                return

            # Max attempts exceeded - treat as terminal
            logger.warning(
                "Task %s exceeded max retry attempts (%d), marking as terminal",
                task_model.name,
                max_attempts,
            )

        # Permanent error or max retries exceeded - mark as terminal
        exception_details = {
            "details": {
                "error": str(exception),
                "errors": [str(exception)],
                "transient": is_transient(exception),
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
