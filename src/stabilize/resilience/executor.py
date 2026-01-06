"""
Resilience executor for Stabilize.

Provides a unified execution function that combines bulkhead and circuit
breaker protection, mapping library exceptions to Stabilize's error types.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TypeVar, cast

from bulkman.config import ExecutionResult
from bulkman.exceptions import (
    BulkheadCircuitOpenError,
    BulkheadFullError,
    BulkheadTimeoutError,
)
from resilient_circuit import CircuitProtectorPolicy
from resilient_circuit.exceptions import ProtectedCallError

from stabilize.errors import TaskTimeoutError, TransientError
from stabilize.resilience.bulkheads import TaskBulkheadManager

logger = logging.getLogger(__name__)

T = TypeVar("T")


def execute_with_resilience(
    bulkhead_manager: TaskBulkheadManager,
    circuit: CircuitProtectorPolicy,
    task_type: str,
    func: Callable[..., T],
    func_args: tuple[Any, ...],
    func_kwargs: dict[str, Any] | None = None,
    timeout: float | None = None,
    task_name: str = "unknown",
    stage_id: str | None = None,
    execution_id: str | None = None,
) -> T:
    """
    Execute a function with bulkhead and circuit breaker protection.

    This function:
    1. Checks the circuit breaker - fails fast if circuit is open
    2. Executes through the bulkhead with timeout
    3. Maps any library exceptions to Stabilize's error types

    Args:
        bulkhead_manager: The bulkhead manager to use
        circuit: The circuit breaker to use
        task_type: The task type (for bulkhead selection)
        func: The function to execute
        func_args: Tuple of positional arguments for the function
        func_kwargs: Dict of keyword arguments for the function
        timeout: Timeout in seconds
        task_name: Name of the task (for error messages)
        stage_id: Stage ID (for error context)
        execution_id: Execution ID (for error context)

    Returns:
        The result of the function

    Raises:
        TransientError: For bulkhead full, circuit open (retry later)
        TaskTimeoutError: For timeout exceeded
        Exception: Any other exception from the function itself
    """
    if func_kwargs is None:
        func_kwargs = {}

    try:
        # Wrap execution with circuit breaker
        @circuit
        def protected_execute() -> ExecutionResult:
            return bulkhead_manager.execute_with_timeout(
                task_type,
                func,
                *func_args,
                timeout=timeout,
                **func_kwargs,
            )

        result = protected_execute()

        # Check if the execution was successful
        if result.success:
            return cast(T, result.result)
        else:
            # Re-raise the original error from the task
            if result.error:
                raise result.error
            else:
                raise RuntimeError(f"Task failed without error: {result}")

    except BulkheadFullError as e:
        # Bulkhead is at capacity - retry later when capacity available
        logger.warning(f"Bulkhead full for task type '{task_type}': {e}")
        raise TransientError(
            f"Bulkhead full for {task_type}: {e}",
            retry_after=5,  # Suggest retry after 5 seconds
            cause=e,
        ) from e

    except (BulkheadCircuitOpenError, ProtectedCallError) as e:
        # Circuit breaker is open - retry after cooldown
        logger.warning(f"Circuit breaker open for task type '{task_type}': {e}")
        raise TransientError(
            f"Circuit breaker open for {task_type}: {e}",
            retry_after=30,  # Suggest retry after cooldown period
            cause=e,
        ) from e

    except BulkheadTimeoutError as e:
        # Task exceeded timeout
        logger.warning(f"Task '{task_name}' timed out: {e}")
        raise TaskTimeoutError(
            f"Task exceeded timeout: {e}",
            task_name=task_name,
            stage_id=stage_id,
            execution_id=execution_id,
            cause=e,
        ) from e


def extract_result_or_raise(
    result: ExecutionResult,
    task_name: str = "unknown",
    stage_id: str | None = None,
    execution_id: str | None = None,
) -> Any:
    """
    Extract the result from an ExecutionResult or raise the appropriate error.

    Args:
        result: The ExecutionResult from bulkhead execution
        task_name: Name of the task (for error messages)
        stage_id: Stage ID (for error context)
        execution_id: Execution ID (for error context)

    Returns:
        The result value if successful

    Raises:
        TaskTimeoutError: If the execution timed out
        Exception: The original error if execution failed
    """
    if result.success:
        return result.result

    error = result.error
    if error is None:
        raise RuntimeError(f"Task '{task_name}' failed without error details")

    # Check if it was a timeout
    if isinstance(error, BulkheadTimeoutError):
        raise TaskTimeoutError(
            f"Task exceeded timeout: {error}",
            task_name=task_name,
            stage_id=stage_id,
            execution_id=execution_id,
            cause=error,
        ) from error

    # Re-raise the original error
    raise error
