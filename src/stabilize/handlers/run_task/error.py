"""Task error handling logic."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.error_codes import classify_error
from stabilize.errors import is_transient, truncate_error
from stabilize.finalizers import get_finalizer_registry
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CompleteTask

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.persistence.transaction import TransactionHelper
    from stabilize.queue.messages import RunTask
    from stabilize.tasks.interface import Task

logger = logging.getLogger(__name__)


def handle_cancellation(
    stage: StageExecution,
    task_model: TaskExecution,
    task: Task,
    message: RunTask,
    repository: WorkflowStore,
    txn_helper: TransactionHelper,
    retry_on_concurrency_error: Callable[[Callable[[], None], str], None],
) -> None:
    """Handle execution cancellation.

    Uses atomic transaction for stage update + message push.
    Retries on ConcurrencyError with exponential backoff when storing stage.

    Args:
        stage: The stage execution
        task_model: The task execution model
        task: The task implementation
        message: The RunTask message
        repository: The workflow store
        txn_helper: Transaction helper for atomic operations
        retry_on_concurrency_error: Function to retry on concurrency errors
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
            fresh_stage = repository.retrieve_stage(message.stage_id)
            if fresh_stage is None:
                logger.error("Stage %s not found during cancellation", message.stage_id)
                # Mark message processed and push CompleteTask to prevent workflow hang
                txn_helper.execute_atomic(
                    source_message=message,
                    messages_to_push=[
                        (
                            CompleteTask(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                                task_id=message.task_id,
                                status=WorkflowStatus.CANCELED,
                            ),
                            None,
                        )
                    ],
                    handler_name="RunTask",
                )
                return

            if result_context:
                fresh_stage.context.update(result_context)
            if result_outputs:
                fresh_stage.outputs.update(result_outputs)

            # Atomic: store stage + mark processed + push CompleteTask together
            txn_helper.execute_atomic(
                stage=fresh_stage,
                source_message=message,
                messages_to_push=[
                    (
                        CompleteTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=message.task_id,
                            status=WorkflowStatus.CANCELED,
                        ),
                        None,
                    )
                ],
                handler_name="RunTask",
            )
        else:
            # No stage modification needed, no retry required
            txn_helper.execute_atomic(
                source_message=message,
                messages_to_push=[
                    (
                        CompleteTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=message.task_id,
                            status=WorkflowStatus.CANCELED,
                        ),
                        None,
                    )
                ],
                handler_name="RunTask",
            )

    retry_on_concurrency_error(do_cancel, f"canceling task {message.task_id}")


def handle_exception(
    stage: StageExecution,
    task_model: TaskExecution,
    task: Task,
    message: RunTask,
    exception: Exception,
    repository: WorkflowStore,
    txn_helper: TransactionHelper,
    get_backoff_fn: Callable[[StageExecution, TaskExecution, RunTask, int], timedelta],
    retry_on_concurrency_error: Callable[[Callable[[], None], str], None],
) -> None:
    """Handle task execution exception.

    Uses is_transient() to determine if the error should be retried.
    Transient errors are rescheduled with exponential backoff.
    Permanent errors are marked as terminal.

    Uses atomic transaction for stage update + message push.

    Args:
        stage: The stage execution
        task_model: The task execution model
        task: The task implementation
        message: The RunTask message
        exception: The exception that occurred
        repository: The workflow store
        txn_helper: Transaction helper for atomic operations
        get_backoff_fn: Function to get backoff period
        retry_on_concurrency_error: Function to retry on concurrency errors
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
            _handle_transient_retry(
                stage,
                task_model,
                message,
                exception,
                current_attempts,
                max_attempts,
                repository,
                txn_helper,
                get_backoff_fn,
                retry_on_concurrency_error,
            )
            return

        # Max attempts exceeded - treat as terminal
        logger.warning(
            "Task %s exceeded max retry attempts (%d), marking as terminal",
            task_model.name,
            max_attempts,
        )

    # Permanent error or max retries exceeded - mark as terminal
    _mark_terminal(
        task_model,
        message,
        exception,
        repository,
        txn_helper,
        retry_on_concurrency_error,
    )


def _handle_transient_retry(
    stage: StageExecution,
    task_model: TaskExecution,
    message: RunTask,
    exception: Exception,
    current_attempts: int,
    max_attempts: int,
    repository: WorkflowStore,
    txn_helper: TransactionHelper,
    get_backoff_fn: Callable[[StageExecution, TaskExecution, RunTask, int], timedelta],
    retry_on_concurrency_error: Callable[[Callable[[], None], str], None],
) -> None:
    """Handle transient error with retry."""
    next_attempt = current_attempts + 1
    delay = get_backoff_fn(stage, task_model, message, next_attempt + 1)

    # Create new message with incremented attempt count
    retry_message = message.copy_with_attempts(next_attempt)

    # Check for context_update from TransientError (stateful retries)
    # Note: bulkman wraps exceptions in BulkheadError, so we need to
    # check __cause__ chain to find the original TransientError
    context_update = getattr(exception, "context_update", None)
    if context_update is None and exception.__cause__ is not None:
        context_update = getattr(exception.__cause__, "context_update", None)

    if context_update:
        logger.debug(
            "Task %s applying context_update on retry: %s",
            task_model.name,
            list(context_update.keys()),
        )

        # CRITICAL: Reload stage from DB before storing to get latest version.
        def do_update_context() -> None:
            fresh_stage = repository.retrieve_stage(message.stage_id)
            if fresh_stage is None:
                logger.warning("Stage %s not found during context update", message.stage_id)
                # Still push retry message even if stage not found
                txn_helper.execute_atomic(
                    messages_to_push=[(retry_message, int(delay.total_seconds()))],
                    handler_name="RunTask",
                )
                return
            fresh_stage.context.update(context_update)
            # Atomic: store stage with context update + push retry message
            txn_helper.execute_atomic(
                stage=fresh_stage,
                messages_to_push=[(retry_message, int(delay.total_seconds()))],
                handler_name="RunTask",
            )

        retry_on_concurrency_error(
            do_update_context,
            f"updating context for task {task_model.name}",
        )
    else:
        # Atomic: push retry message (no stage update needed)
        txn_helper.execute_atomic(
            messages_to_push=[(retry_message, int(delay.total_seconds()))],
            handler_name="RunTask",
        )

    logger.debug(
        "Task %s rescheduled for retry %d/%d with delay %s",
        task_model.name,
        next_attempt + 1,
        max_attempts,
        delay,
    )


def _mark_terminal(
    task_model: TaskExecution,
    message: RunTask,
    exception: Exception,
    repository: WorkflowStore,
    txn_helper: TransactionHelper,
    retry_on_concurrency_error: Callable[[Callable[[], None], str], None],
) -> None:
    """Mark task as terminal due to permanent error or max retries exceeded."""
    # Get error code for classification
    error_code = classify_error(exception)

    # Truncate error message to prevent oversized storage
    error_str = truncate_error(str(exception))

    exception_details = {
        "details": {
            "error": error_str,
            "errors": [error_str],
            "transient": is_transient(exception),
            "error_code": error_code.value,
        }
    }

    # Update task model (not persisted via stage, so no concurrency issue)
    task_model.task_exception_details["exception"] = exception_details

    def do_mark_terminal() -> None:
        # Re-fetch stage to get current version on each retry attempt
        fresh_stage = repository.retrieve_stage(message.stage_id)
        if fresh_stage is None:
            logger.error("Stage %s not found during exception handling", message.stage_id)
            # Mark message processed and push CompleteTask to prevent workflow hang
            txn_helper.execute_atomic_critical(
                source_message=message,
                messages_to_push=[
                    (
                        CompleteTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=message.task_id,
                            status=WorkflowStatus.TERMINAL,
                        ),
                        None,
                    )
                ],
                handler_name="RunTask_ErrorHandling",
            )
            return

        fresh_stage.context["exception"] = exception_details
        status = fresh_stage.failure_status(default=WorkflowStatus.TERMINAL)

        # Execute any registered finalizers for this stage
        try:
            registry = get_finalizer_registry()
            finalizer_results = registry.execute(fresh_stage.id, timeout=30.0)
            if finalizer_results:
                failed_finalizers = [r for r in finalizer_results if not r.success]
                if failed_finalizers:
                    logger.warning(
                        "Stage %s: %d/%d finalizers failed",
                        fresh_stage.id,
                        len(failed_finalizers),
                        len(finalizer_results),
                    )
        except Exception as e:
            logger.warning(
                "Error executing finalizers for stage %s: %s",
                fresh_stage.id,
                e,
            )

        # Atomic: store stage + mark processed + push CompleteTask together
        txn_helper.execute_atomic_critical(
            stage=fresh_stage,
            source_message=message,
            messages_to_push=[
                (
                    CompleteTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        status=status,
                        original_status=WorkflowStatus.TERMINAL,
                    ),
                    None,
                )
            ],
            handler_name="RunTask_ErrorHandling",
        )

    retry_on_concurrency_error(do_mark_terminal, f"marking task {message.task_id} terminal")


def complete_with_error(
    stage: StageExecution,
    task_model: TaskExecution,
    message: RunTask,
    error: str,
    repository: WorkflowStore,
    txn_helper: TransactionHelper,
    retry_on_concurrency_error: Callable[[Callable[[], None], str], None],
) -> None:
    """Complete task with an error.

    Uses atomic transaction for stage update + message push.
    Retries on ConcurrencyError with exponential backoff.

    Args:
        stage: The stage execution
        task_model: The task execution model
        message: The RunTask message
        error: The error message
        repository: The workflow store
        txn_helper: Transaction helper for atomic operations
        retry_on_concurrency_error: Function to retry on concurrency errors
    """
    # Truncate error message to prevent oversized storage
    truncated_error = truncate_error(error)

    def do_complete() -> None:
        # Re-fetch stage to get current version on each retry attempt
        fresh_stage = repository.retrieve_stage(message.stage_id)
        if fresh_stage is None:
            logger.error("Stage %s not found during error completion", message.stage_id)
            # Mark message processed and push CompleteTask to prevent workflow hang
            txn_helper.execute_atomic_critical(
                source_message=message,
                messages_to_push=[
                    (
                        CompleteTask(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                            task_id=message.task_id,
                            status=WorkflowStatus.TERMINAL,
                        ),
                        None,
                    )
                ],
                handler_name="RunTask_ErrorHandling",
            )
            return

        fresh_stage.context["exception"] = {
            "details": {"error": truncated_error},
        }

        # Atomic: store stage + mark processed + push CompleteTask together
        txn_helper.execute_atomic_critical(
            stage=fresh_stage,
            source_message=message,
            messages_to_push=[
                (
                    CompleteTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        status=WorkflowStatus.TERMINAL,
                    ),
                    None,
                )
            ],
            handler_name="RunTask_ErrorHandling",
        )

    retry_on_concurrency_error(do_complete, f"completing task {message.task_id} with error")
