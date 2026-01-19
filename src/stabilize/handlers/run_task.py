"""
RunTaskHandler - executes tasks.

This is the handler that actually runs task implementations.
It handles execution, retries, timeouts, and result processing.

Uses bulkman for bulkhead pattern (per-task-type isolation) and
resilient_circuit for circuit breaker protection.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import TYPE_CHECKING

from resilient_circuit import ExponentialDelay

from stabilize.errors import TaskTimeoutError, is_transient
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteTask,
    PauseTask,
    RunTask,
)
from stabilize.resilience.bulkheads import TaskBulkheadManager
from stabilize.resilience.circuits import WorkflowCircuitFactory
from stabilize.resilience.config import HandlerConfig, ResilienceConfig
from stabilize.resilience.executor import execute_with_resilience
from stabilize.resilience.process_executor import ProcessIsolatedTaskExecutor
from stabilize.tasks.interface import RetryableTask, Task
from stabilize.tasks.registry import TaskNotFoundError, TaskRegistry
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue

logger = logging.getLogger(__name__)


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
        retry_delay: timedelta | None = None,
        bulkhead_manager: TaskBulkheadManager | None = None,
        circuit_factory: WorkflowCircuitFactory | None = None,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config)
        self.task_registry = task_registry

        # Create backoff calculator from config
        self._task_backoff = ExponentialDelay(
            min_delay=timedelta(milliseconds=self.handler_config.task_backoff_min_delay_ms),
            max_delay=timedelta(milliseconds=self.handler_config.task_backoff_max_delay_ms),
            factor=self.handler_config.concurrency_backoff_factor,
            jitter=self.handler_config.concurrency_jitter,
        )

        # Check isolation mode
        self.isolation_mode = os.environ.get("STABILIZE_ISOLATION_MODE", "thread").lower()
        self.process_executor = ProcessIsolatedTaskExecutor() if self.isolation_mode == "process" else None

        # Initialize resilience components with defaults if not provided
        if bulkhead_manager is None or circuit_factory is None:
            config = ResilienceConfig.from_env()
            self.bulkhead_manager = bulkhead_manager or TaskBulkheadManager(config)
            self.circuit_factory = circuit_factory or WorkflowCircuitFactory(config)
        else:
            self.bulkhead_manager = bulkhead_manager
            self.circuit_factory = circuit_factory

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
                # Atomic: mark message processed + push CompleteTask
                with self.repository.transaction(self.queue) as txn:
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="RunTask",
                            execution_id=message.execution_id,
                        )
                    txn.push_message(
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
                # Atomic: mark message processed + push PauseTask
                with self.repository.transaction(self.queue) as txn:
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="RunTask",
                            execution_id=message.execution_id,
                        )
                    txn.push_message(
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
                # Atomic: mark message processed + push CompleteTask
                with self.repository.transaction(self.queue) as txn:
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="RunTask",
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

            # Execute the task with timeout enforcement
            try:
                timeout = self._get_task_timeout(stage, task)
                result = self._execute_with_timeout(task, stage, timeout, message)
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

        Returns the dynamic timeout for RetryableTask or configured default for others.
        The default is configurable via STABILIZE_DEFAULT_TASK_TIMEOUT_S env var.
        """
        if isinstance(task, RetryableTask):
            return task.get_dynamic_timeout(stage)
        return timedelta(seconds=self.handler_config.default_task_timeout_seconds)

    def _execute_with_timeout(
        self,
        task: Task,
        stage: StageExecution,
        timeout: timedelta,
        message: RunTask,
    ) -> TaskResult:
        """Execute a task with timeout enforcement via bulkhead.

        Uses bulkhead for thread pool management and circuit breaker for
        failure protection. Runs the task through the appropriate bulkhead
        based on task type.

        Args:
            task: The task to execute
            stage: The stage execution context
            timeout: Maximum time allowed for execution
            message: The RunTask message (for task type info)

        Returns:
            The task result

        Raises:
            TaskTimeoutError: If the task exceeds the timeout
            TransientError: If bulkhead is full or circuit is open
        """
        task_name = getattr(task, "name", type(task).__name__)
        execution_id = stage.execution.id if stage.execution else None

        # Get circuit breaker for this workflow + task type
        circuit = self.circuit_factory.get_circuit(
            workflow_execution_id=execution_id or "unknown",
            task_type=message.task_type,
        )

        # Define the execution function (process-isolated or direct)
        def execute_task(s: StageExecution) -> TaskResult:
            if self.process_executor:
                # Pass timeout directly to avoid race conditions with concurrent tasks
                return self.process_executor.execute(task, s, timeout_seconds=timeout.total_seconds())
            return task.execute(s)

        # Execute through bulkhead with circuit breaker protection
        return execute_with_resilience(
            bulkhead_manager=self.bulkhead_manager,
            circuit=circuit,
            task_type=message.task_type,
            func=execute_task,
            func_args=(stage,),
            timeout=timeout.total_seconds(),
            task_name=task_name,
            stage_id=stage.id,
            execution_id=execution_id,
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
        # Store outputs in stage (with defensive type checks for user-defined tasks)
        if result.context and isinstance(result.context, dict):
            stage.context.update(result.context)
        if result.outputs and isinstance(result.outputs, dict):
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
            # Atomic: store stage + mark processed + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="RunTask",
                        execution_id=message.execution_id,
                    )
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

            # Atomic: store stage + mark processed + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="RunTask",
                        execution_id=message.execution_id,
                    )
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

            # Atomic: store stage + mark processed + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="RunTask",
                        execution_id=message.execution_id,
                    )
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
            # Unhandled status - treat as error to prevent workflow hang
            logger.warning(
                "Unhandled task status %s for task %s, treating as TERMINAL",
                result.status,
                task_model.name,
            )
            status = stage.failure_status(default=WorkflowStatus.TERMINAL)

            # Atomic: store stage + mark processed + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="RunTask",
                        execution_id=message.execution_id,
                    )
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

    def _get_backoff_period(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        message: RunTask,
        attempt: int = 1,
    ) -> timedelta:
        """Calculate backoff period for retry with exponential backoff and jitter.

        Uses configurable ExponentialDelay for consistent backoff calculation.
        Defaults can be customized via STABILIZE_TASK_BACKOFF_MIN_MS and
        STABILIZE_TASK_BACKOFF_MAX_MS environment variables.

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

        # Use configured ExponentialDelay for consistent backoff
        # for_attempt() returns seconds as float, convert to timedelta
        return timedelta(seconds=self._task_backoff.for_attempt(attempt))

    def _handle_cancellation(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        task: Task,
        message: RunTask,
    ) -> None:
        """Handle execution cancellation.

        Uses atomic transaction for stage update + message push.
        Retries on ConcurrencyError with exponential backoff when storing stage.
        """
        result = task.on_cancel(stage) if hasattr(task, "on_cancel") else None

        # Extract context/outputs to apply on retry (if result exists)
        result_context = None
        result_outputs = None
        if result:
            if result.context and isinstance(result.context, dict):
                result_context = result.context
            if result.outputs and isinstance(result.outputs, dict):
                result_outputs = result.outputs

        def do_cancel() -> None:
            if result_context or result_outputs:
                # Re-fetch stage to get current version on each retry attempt
                fresh_stage = self.repository.retrieve_stage(message.stage_id)
                if fresh_stage is None:
                    logger.error("Stage %s not found during cancellation", message.stage_id)
                    # Mark message processed and push CompleteTask to prevent workflow hang
                    with self.repository.transaction(self.queue) as txn:
                        if message.message_id:
                            txn.mark_message_processed(
                                message_id=message.message_id,
                                handler_type="RunTask",
                                execution_id=message.execution_id,
                            )
                        txn.push_message(
                            CompleteTask(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                                task_id=message.task_id,
                                status=WorkflowStatus.CANCELED,
                            )
                        )
                    return

                if result_context:
                    fresh_stage.context.update(result_context)
                if result_outputs:
                    fresh_stage.outputs.update(result_outputs)

                # Atomic: store stage + mark processed + push CompleteTask together
                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(fresh_stage)
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="RunTask",
                            execution_id=message.execution_id,
                        )
                    txn.push_message(
                        CompleteTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=message.task_id,
                            status=WorkflowStatus.CANCELED,
                        )
                    )
            else:
                # No stage modification needed, no retry required
                with self.repository.transaction(self.queue) as txn:
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="RunTask",
                            execution_id=message.execution_id,
                        )
                    txn.push_message(
                        CompleteTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=message.task_id,
                            status=WorkflowStatus.CANCELED,
                        )
                    )

        self.retry_on_concurrency_error(do_cancel, f"canceling task {message.task_id}")

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

            # Get attempt count from message (0-indexed) and increment
            current_attempts = message.attempts or 0
            max_attempts = message.max_attempts or 10

            if current_attempts + 1 < max_attempts:
                # Reschedule with backoff
                next_attempt = current_attempts + 1
                delay = self._get_backoff_period(stage, task_model, message, next_attempt + 1)

                # Create new message with incremented attempt count
                retry_message = message.copy_with_attempts(next_attempt)

                # Atomic: push retry message
                with self.repository.transaction(self.queue) as txn:
                    txn.push_message(retry_message, int(delay.total_seconds()))

                logger.debug(
                    "Task %s rescheduled for retry %d/%d with delay %s",
                    task_model.name,
                    next_attempt + 1,
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

        # Update task model (not persisted via stage, so no concurrency issue)
        task_model.task_exception_details["exception"] = exception_details

        def do_mark_terminal() -> None:
            # Re-fetch stage to get current version on each retry attempt
            fresh_stage = self.repository.retrieve_stage(message.stage_id)
            if fresh_stage is None:
                logger.error("Stage %s not found during exception handling", message.stage_id)
                # Mark message processed and push CompleteTask to prevent workflow hang
                with self.repository.transaction(self.queue) as txn:
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="RunTask",
                            execution_id=message.execution_id,
                        )
                    txn.push_message(
                        CompleteTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=message.task_id,
                            status=WorkflowStatus.TERMINAL,
                        )
                    )
                return

            fresh_stage.context["exception"] = exception_details
            status = fresh_stage.failure_status(default=WorkflowStatus.TERMINAL)

            # Atomic: store stage + mark processed + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(fresh_stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="RunTask",
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

        self.retry_on_concurrency_error(do_mark_terminal, f"marking task {message.task_id} terminal")

    def _complete_with_error(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        message: RunTask,
        error: str,
    ) -> None:
        """Complete task with an error.

        Uses atomic transaction for stage update + message push.
        Retries on ConcurrencyError with exponential backoff.
        """

        def do_complete() -> None:
            # Re-fetch stage to get current version on each retry attempt
            fresh_stage = self.repository.retrieve_stage(message.stage_id)
            if fresh_stage is None:
                logger.error("Stage %s not found during error completion", message.stage_id)
                # Mark message processed and push CompleteTask to prevent workflow hang
                with self.repository.transaction(self.queue) as txn:
                    if message.message_id:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="RunTask",
                            execution_id=message.execution_id,
                        )
                    txn.push_message(
                        CompleteTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=message.task_id,
                            status=WorkflowStatus.TERMINAL,
                        )
                    )
                return

            fresh_stage.context["exception"] = {
                "details": {"error": error},
            }

            # Atomic: store stage + mark processed + push CompleteTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(fresh_stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="RunTask",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    CompleteTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        status=WorkflowStatus.TERMINAL,
                    )
                )

        self.retry_on_concurrency_error(do_complete, f"completing task {message.task_id} with error")
