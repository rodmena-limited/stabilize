"""Task result processing logic."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING

from resilient_circuit import ExponentialDelay

from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CompleteTask, JumpToStage
from stabilize.tasks.interface import RetryableTask

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.transaction import TransactionHelper
    from stabilize.queue.messages import RunTask
    from stabilize.tasks.registry import TaskRegistry
    from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)


def process_result(
    stage: StageExecution,
    task_model: TaskExecution,
    result: TaskResult,
    message: RunTask,
    txn_helper: TransactionHelper,
    get_backoff_fn: Callable[[StageExecution, TaskExecution, RunTask, int], timedelta],
) -> None:
    """Process a task result.

    Uses atomic transactions to ensure stage updates and message pushes
    are committed together. This prevents orphaned workflows where the
    stage is saved but the continuation message is lost.

    Args:
        stage: The stage execution
        task_model: The task execution model
        result: The task result to process
        message: The original RunTask message
        txn_helper: Transaction helper for atomic operations
        get_backoff_fn: Function to get backoff period for retries
    """
    # Store outputs in stage (with defensive type checks for user-defined tasks)
    if result.context and isinstance(result.context, dict):
        stage.context.update(result.context)
    if result.outputs and isinstance(result.outputs, dict):
        stage.outputs.update(result.outputs)

    # Handle based on status - use atomic transactions
    if result.status == WorkflowStatus.RUNNING:
        _handle_running(stage, task_model, message, txn_helper, get_backoff_fn)

    elif result.status == WorkflowStatus.REDIRECT and result.target_stage_ref_id:
        _handle_redirect(stage, task_model, result, message, txn_helper)

    elif result.status in {
        WorkflowStatus.SUCCEEDED,
        WorkflowStatus.REDIRECT,
        WorkflowStatus.SKIPPED,
        WorkflowStatus.FAILED_CONTINUE,
        WorkflowStatus.STOPPED,
    }:
        _handle_success_like(stage, result, message, txn_helper)

    elif result.status == WorkflowStatus.CANCELED:
        _handle_canceled(stage, result, message, txn_helper)

    elif result.status == WorkflowStatus.TERMINAL:
        _handle_terminal(stage, result, message, txn_helper)

    else:
        _handle_unhandled(stage, task_model, result, message, txn_helper)


def _handle_running(
    stage: StageExecution,
    task_model: TaskExecution,
    message: RunTask,
    txn_helper: TransactionHelper,
    get_backoff_fn: Callable[[StageExecution, TaskExecution, RunTask, int], timedelta],
) -> None:
    """Handle RUNNING status - task needs to be re-executed."""
    delay = get_backoff_fn(stage, task_model, message, 1)

    # Atomic: store stage + push message together
    txn_helper.execute_atomic(
        stage=stage,
        messages_to_push=[(message, int(delay.total_seconds()))],
        handler_name="RunTask",
    )

    logger.debug(
        "Task %s still running, re-queuing with %s delay",
        task_model.name,
        delay,
    )


def _handle_redirect(
    stage: StageExecution,
    task_model: TaskExecution,
    result: TaskResult,
    message: RunTask,
    txn_helper: TransactionHelper,
) -> None:
    """Handle REDIRECT status - dynamic routing to a different stage."""
    # target_stage_ref_id is guaranteed non-None when this function is called
    # (checked at the call site before invoking _handle_redirect)
    assert result.target_stage_ref_id is not None

    logger.info(
        "Task %s requested jump to stage %s",
        task_model.name,
        result.target_stage_ref_id,
    )

    # Atomic: store stage + mark processed + push JumpToStage + CompleteTask
    txn_helper.execute_atomic(
        stage=stage,
        source_message=message,
        messages_to_push=[
            (
                JumpToStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    target_stage_ref_id=result.target_stage_ref_id,
                    jump_context=result.context or {},
                    jump_outputs=result.outputs or {},
                ),
                None,
            ),
            (
                CompleteTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    status=result.status,
                ),
                None,
            ),
        ],
        handler_name="RunTask",
    )


def _handle_success_like(
    stage: StageExecution,
    result: TaskResult,
    message: RunTask,
    txn_helper: TransactionHelper,
) -> None:
    """Handle success-like statuses (SUCCEEDED, SKIPPED, FAILED_CONTINUE, STOPPED)."""
    txn_helper.execute_atomic(
        stage=stage,
        source_message=message,
        messages_to_push=[
            (
                CompleteTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    status=result.status,
                ),
                None,
            )
        ],
        handler_name="RunTask",
    )


def _handle_canceled(
    stage: StageExecution,
    result: TaskResult,
    message: RunTask,
    txn_helper: TransactionHelper,
) -> None:
    """Handle CANCELED status."""
    status = stage.failure_status(default=result.status)

    txn_helper.execute_atomic(
        stage=stage,
        source_message=message,
        messages_to_push=[
            (
                CompleteTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    status=status,
                    original_status=result.status,
                ),
                None,
            )
        ],
        handler_name="RunTask",
    )


def _handle_terminal(
    stage: StageExecution,
    result: TaskResult,
    message: RunTask,
    txn_helper: TransactionHelper,
) -> None:
    """Handle TERMINAL status."""
    status = stage.failure_status(default=result.status)

    txn_helper.execute_atomic(
        stage=stage,
        source_message=message,
        messages_to_push=[
            (
                CompleteTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    status=status,
                    original_status=result.status,
                ),
                None,
            )
        ],
        handler_name="RunTask",
    )


def _handle_unhandled(
    stage: StageExecution,
    task_model: TaskExecution,
    result: TaskResult,
    message: RunTask,
    txn_helper: TransactionHelper,
) -> None:
    """Handle unhandled status - treat as error to prevent workflow hang."""
    logger.warning(
        "Unhandled task status %s for task %s, treating as TERMINAL",
        result.status,
        task_model.name,
    )
    status = stage.failure_status(default=WorkflowStatus.TERMINAL)

    txn_helper.execute_atomic(
        stage=stage,
        source_message=message,
        messages_to_push=[
            (
                CompleteTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    status=status,
                    original_status=result.status,
                ),
                None,
            )
        ],
        handler_name="RunTask",
    )


def get_backoff_period(
    stage: StageExecution,
    task_model: TaskExecution,
    message: RunTask,
    attempt: int,
    task_registry: TaskRegistry,
    task_backoff: ExponentialDelay,
    current_time_fn: Callable[[], int],
) -> timedelta:
    """Calculate backoff period for retry with exponential backoff and jitter.

    Uses configurable ExponentialDelay for consistent backoff calculation.

    Args:
        stage: The stage execution context
        task_model: The task execution model
        message: The RunTask message
        attempt: The current attempt number (1-based)
        task_registry: Registry for looking up task implementations
        task_backoff: Configured exponential delay calculator
        current_time_fn: Function to get current time in milliseconds

    Returns:
        The calculated backoff period
    """
    from stabilize.handlers.run_task.execution import resolve_task
    from stabilize.tasks.registry import TaskNotFoundError

    # Try to get the task and use its backoff if it's a RetryableTask
    try:
        task = resolve_task(message.task_type, task_model, task_registry)
        if isinstance(task, RetryableTask):
            elapsed = timedelta(milliseconds=current_time_fn() - (task_model.start_time or 0))
            return task.get_dynamic_backoff_period(stage, elapsed)
    except TaskNotFoundError:
        pass

    # Use configured ExponentialDelay for consistent backoff
    return timedelta(seconds=task_backoff.for_attempt(attempt))
