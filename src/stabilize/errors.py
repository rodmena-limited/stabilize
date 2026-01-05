"""Stabilize error hierarchy.

Two-tier exception hierarchy following DBOS patterns:

1. StabilizeBaseException - Base for all errors, not caught by default handlers
2. StabilizeError - Standard errors that can be caught and handled

Error Classification:
- TransientError: Retryable (network issues, timeouts, 5xx responses)
- PermanentError: Non-retryable (auth failures, validation errors, 4xx)
- RecoveryError: Crash recovery failed
- ConfigurationError: Invalid configuration
- TaskError: Task execution failed
- WorkflowError: Workflow-level errors

Usage:
    from stabilize.errors import TransientError, PermanentError

    try:
        response = httpx.get(url)
        if response.status_code >= 500:
            raise TransientError("Server error", code=response.status_code)
        elif response.status_code >= 400:
            raise PermanentError("Client error", code=response.status_code)
    except TransientError:
        # Will be retried
        raise
    except PermanentError:
        # Will not be retried, move to DLQ
        raise
"""

from __future__ import annotations

from typing import Any


class StabilizeBaseException(Exception):  # noqa: N818 - intentional base exception name
    """Base exception for all Stabilize errors.

    This is the root of the exception hierarchy. Use this when you want
    an exception that bypasses all default error handling.

    Not caught by:
    - Retry logic
    - Default exception handlers
    - Error recovery

    Use Cases:
    - Critical errors that should crash the process
    - Security violations
    - Invariant violations
    """

    code: int = 0

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            message: Human-readable error message
            code: Optional error code for programmatic handling
            cause: Optional original exception that caused this error
        """
        super().__init__(message)
        if code is not None:
            self.code = code
        self.cause = cause

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.code:
            parts.append(f"(code={self.code})")
        if self.cause:
            parts.append(f"caused by: {self.cause}")
        return " ".join(parts)


class StabilizeError(StabilizeBaseException):
    """Standard Stabilize error.

    Caught by default error handlers and may be retried depending on type.
    All normal application errors should inherit from this.
    """

    code: int = 100


class TransientError(StabilizeError):
    """Retryable errors.

    These errors indicate temporary conditions that may resolve on retry:
    - Network timeouts
    - Connection refused
    - Server errors (5xx)
    - Rate limiting (429)
    - Database lock contention

    The message processor will automatically retry these errors with backoff.
    """

    code: int = 101

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
        retry_after: float | None = None,
    ) -> None:
        """Initialize transient error.

        Args:
            message: Human-readable error message
            code: Optional error code
            cause: Optional original exception
            retry_after: Optional seconds to wait before retry
        """
        super().__init__(message, code=code, cause=cause)
        self.retry_after = retry_after


class PermanentError(StabilizeError):
    """Non-retryable errors.

    These errors indicate conditions that will not resolve on retry:
    - Authentication failures (401)
    - Authorization failures (403)
    - Not found (404)
    - Validation errors (400)
    - Business logic violations

    The message will be moved to the Dead Letter Queue (DLQ).
    """

    code: int = 102


class RecoveryError(StabilizeError):
    """Crash recovery failed.

    Raised when the system cannot recover a workflow after restart:
    - Corrupted state
    - Missing idempotency data
    - Incompatible schema version

    May require manual intervention to resolve.
    """

    code: int = 103


class ConfigurationError(StabilizeError):
    """Invalid configuration.

    Raised during initialization when configuration is invalid:
    - Missing required settings
    - Invalid connection strings
    - Incompatible options

    Usually indicates a deployment or setup issue.
    """

    code: int = 104


class TaskError(StabilizeError):
    """Task execution failed.

    Raised when a task fails during execution:
    - Task timeout
    - Task implementation error
    - Missing task dependencies

    Contains details about which task failed and why.
    """

    code: int = 200

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
        task_name: str | None = None,
        stage_id: str | None = None,
        execution_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize task error.

        Args:
            message: Human-readable error message
            code: Optional error code
            cause: Optional original exception
            task_name: Name of the failed task
            stage_id: ID of the stage containing the task
            execution_id: ID of the workflow execution
            details: Additional error details
        """
        super().__init__(message, code=code, cause=cause)
        self.task_name = task_name
        self.stage_id = stage_id
        self.execution_id = execution_id
        self.details = details or {}


class TaskTimeoutError(TaskError):
    """Task timed out.

    Raised when a task exceeds its configured timeout.
    May be transient if the task can be retried.
    """

    code: int = 201


class TaskNotFoundError(TaskError):
    """Task implementation not found.

    Raised when the task registry cannot find the implementation.
    Usually indicates a configuration or deployment issue.
    """

    code: int = 202


class WorkflowError(StabilizeError):
    """Workflow-level error.

    Raised for workflow-level issues:
    - Workflow not found
    - Invalid workflow state
    - Workflow already complete

    Contains the execution ID for troubleshooting.
    """

    code: int = 300

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Initialize workflow error.

        Args:
            message: Human-readable error message
            code: Optional error code
            cause: Optional original exception
            execution_id: ID of the workflow execution
        """
        super().__init__(message, code=code, cause=cause)
        self.execution_id = execution_id


class WorkflowNotFoundError(WorkflowError):
    """Workflow not found.

    Raised when attempting to retrieve a non-existent workflow.
    """

    code: int = 301


class StageError(StabilizeError):
    """Stage-level error.

    Raised for stage-level issues:
    - Stage not found
    - Invalid stage state
    - Stage dependency failure
    """

    code: int = 400

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
        stage_id: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Initialize stage error.

        Args:
            message: Human-readable error message
            code: Optional error code
            cause: Optional original exception
            stage_id: ID of the stage
            execution_id: ID of the workflow execution
        """
        super().__init__(message, code=code, cause=cause)
        self.stage_id = stage_id
        self.execution_id = execution_id


class QueueError(StabilizeError):
    """Queue operation error.

    Raised for queue-level issues:
    - Message push failed
    - Message poll failed
    - Queue connection lost
    """

    code: int = 500


class DeadLetterError(QueueError):
    """Dead letter queue error.

    Raised when a message is moved to the DLQ:
    - Max attempts exceeded
    - Permanent error encountered
    """

    code: int = 501


def is_transient(error: Exception) -> bool:
    """Check if an error is transient and should be retried.

    Args:
        error: The exception to check

    Returns:
        True if the error is transient and should be retried
    """
    if isinstance(error, TransientError):
        return True

    # Check for common transient errors from other libraries
    error_name = type(error).__name__.lower()
    transient_patterns = [
        "timeout",
        "connection",
        "temporary",
        "unavailable",
        "retry",
        "throttl",
        "ratelimit",
    ]
    return any(pattern in error_name for pattern in transient_patterns)


def is_permanent(error: Exception) -> bool:
    """Check if an error is permanent and should not be retried.

    Args:
        error: The exception to check

    Returns:
        True if the error is permanent and should not be retried
    """
    if isinstance(error, PermanentError):
        return True

    # Check for common permanent errors
    error_name = type(error).__name__.lower()
    permanent_patterns = [
        "validation",
        "authentication",
        "authorization",
        "forbidden",
        "notfound",
        "invalid",
    ]
    return any(pattern in error_name for pattern in permanent_patterns)
