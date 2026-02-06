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

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.error_codes import ErrorCode


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

    Attributes:
        code: Numeric error code for programmatic handling
        error_code: Semantic ErrorCode for categorization and routing
        cause: Optional original exception that caused this error
    """

    code: int = 0
    _error_code: ErrorCode | None = None

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
        error_code: ErrorCode | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            message: Human-readable error message
            code: Optional numeric error code for programmatic handling
            cause: Optional original exception that caused this error
            error_code: Optional semantic ErrorCode for categorization
        """
        super().__init__(message)
        if code is not None:
            self.code = code
        self.cause = cause
        if error_code is not None:
            self._error_code = error_code

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception.

        Returns the explicitly set error_code if available, otherwise
        returns a default based on the exception class.
        """
        if self._error_code is not None:
            return self._error_code
        # Import here to avoid circular imports
        from stabilize.error_codes import ErrorCode

        return ErrorCode.UNKNOWN

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

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.SYSTEM_ERROR


class TransientError(StabilizeError):
    """Retryable errors.

    These errors indicate temporary conditions that may resolve on retry:
    - Network timeouts
    - Connection refused
    - Server errors (5xx)
    - Rate limiting (429)
    - Database lock contention

    The message processor will automatically retry these errors with backoff.

    The optional context_update parameter allows preserving state across retry
    attempts. When provided, the context will be merged into the stage context
    before the retry, allowing tasks to track progress across failures.

    Example:
        raise TransientError(
            "Rate limited",
            retry_after=30,
            context_update={"processed_items": 50}
        )
    """

    code: int = 101

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.NETWORK_ERROR

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
        error_code: ErrorCode | None = None,
        retry_after: float | None = None,
        context_update: dict[str, Any] | None = None,
    ) -> None:
        """Initialize transient error.

        Args:
            message: Human-readable error message
            code: Optional error code
            cause: Optional original exception
            error_code: Optional semantic ErrorCode for categorization
            retry_after: Optional seconds to wait before retry
            context_update: Optional dict to merge into stage context on retry.
                           Allows preserving progress across transient failures.
        """
        super().__init__(message, code=code, cause=cause, error_code=error_code)
        self.retry_after = retry_after
        self.context_update = context_update or {}


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

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.USER_CODE_ERROR


class RecoveryError(StabilizeError):
    """Crash recovery failed.

    Raised when the system cannot recover a workflow after restart:
    - Corrupted state
    - Missing idempotency data
    - Incompatible schema version

    May require manual intervention to resolve.
    """

    code: int = 103

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.RECOVERY_FAILED


class ConfigurationError(StabilizeError):
    """Invalid configuration.

    Raised during initialization when configuration is invalid:
    - Missing required settings
    - Invalid connection strings
    - Incompatible options

    Usually indicates a deployment or setup issue.
    """

    code: int = 104

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.CONFIGURATION_INVALID


class ConcurrencyError(TransientError):
    """Optimistic locking failure.

    Raised when a database update fails because the version changed
    concurrently. This is a transient error and should be retried.
    """

    code: int = 105

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.CONCURRENCY_CONFLICT


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
        error_code: ErrorCode | None = None,
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
            error_code: Optional semantic ErrorCode for categorization
            task_name: Name of the failed task
            stage_id: ID of the stage containing the task
            execution_id: ID of the workflow execution
            details: Additional error details
        """
        super().__init__(message, code=code, cause=cause, error_code=error_code)
        self.task_name = task_name
        self.stage_id = stage_id
        self.execution_id = execution_id
        self.details = details or {}

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.USER_CODE_ERROR


class TaskTimeoutError(TaskError):
    """Task timed out.

    Raised when a task exceeds its configured timeout.
    May be transient if the task can be retried.
    """

    code: int = 201

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.TASK_TIMEOUT


class TaskNotFoundError(TaskError):
    """Task implementation not found.

    Raised when the task registry cannot find the implementation.
    Usually indicates a configuration or deployment issue.
    """

    code: int = 202

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.TASK_NOT_FOUND


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


class VerificationError(StabilizeError):
    """Verification failed.

    Raised when verification of stage outputs fails.
    This is the base class for verification errors and is treated as permanent (terminal).

    For transient verification failures that should be retried, use TransientVerificationError.
    """

    code: int = 600

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
        error_code: ErrorCode | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, cause=cause, error_code=error_code)
        self.details = details or {}

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
        from stabilize.error_codes import ErrorCode

        return ErrorCode.VERIFICATION_FAILED


class TransientVerificationError(VerificationError, TransientError):
    """Transient verification failure that should be retried.

    Raised when verification cannot complete yet but may succeed later:
    - External service temporarily unavailable
    - Resource not yet propagated
    - Timing-dependent validation

    The message processor will automatically retry these errors with backoff.
    """

    code: int = 601

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        cause: Exception | None = None,
        details: dict[str, Any] | None = None,
        retry_after: float | None = None,
    ) -> None:
        # Call VerificationError.__init__ for details
        VerificationError.__init__(self, message, code=code, cause=cause, details=details)
        # Store retry_after from TransientError
        self.retry_after = retry_after


def is_transient(error: Exception) -> bool:
    """Check if an error is transient and should be retried.

    Checks the error itself and its cause chain (__cause__) for transient errors.
    This handles cases where libraries wrap errors (e.g., BulkheadError wrapping
    a TransientError).

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
    if any(pattern in error_name for pattern in transient_patterns):
        return True

    # Check the cause chain for wrapped transient errors
    cause = getattr(error, "__cause__", None)
    if cause is not None and cause is not error:
        return is_transient(cause)

    return False


def is_permanent(error: Exception) -> bool:
    """Check if an error is permanent and should not be retried.

    Checks the error itself and its cause chain (__cause__) for permanent errors.
    This handles cases where libraries wrap errors.

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
    if any(pattern in error_name for pattern in permanent_patterns):
        return True

    # Check the cause chain for wrapped permanent errors
    cause = getattr(error, "__cause__", None)
    if cause is not None and cause is not error:
        return is_permanent(cause)

    return False


def truncate_error(message: str, max_bytes: int = 102_400) -> str:
    """Truncate error message to max_bytes, appending '[TRUNCATED]' marker.

    This prevents oversized error messages from bloating the database
    or causing memory issues when persisting exception details.

    The truncation is byte-aware to handle UTF-8 encoded strings properly.
    The '[TRUNCATED]' marker is appended to indicate that the message
    was shortened.

    Args:
        message: The error message to truncate
        max_bytes: Maximum size in bytes (default: 100KB)

    Returns:
        Original message if within limit, otherwise truncated message
        with '[TRUNCATED]' marker.

    Example:
        >>> truncate_error("x" * 200_000)
        'xxxxxxx...[TRUNCATED]'
    """
    if not message:
        return message

    # Encode to bytes to check actual size
    encoded = message.encode("utf-8", errors="replace")

    if len(encoded) <= max_bytes:
        return message

    # Reserve space for the marker
    marker = " [TRUNCATED]"
    marker_bytes = len(marker.encode("utf-8"))
    target_bytes = max_bytes - marker_bytes

    if target_bytes <= 0:
        return marker.strip()

    # Truncate at byte boundary, being careful not to cut in middle of UTF-8 char
    truncated_bytes = encoded[:target_bytes]

    # Decode back, replacing any incomplete UTF-8 sequences at the end
    try:
        truncated = truncated_bytes.decode("utf-8", errors="ignore")
    except UnicodeDecodeError:
        # Fallback: try progressively shorter slices
        for i in range(1, 5):
            try:
                truncated = truncated_bytes[:-i].decode("utf-8")
                break
            except UnicodeDecodeError:
                continue
        else:
            truncated = truncated_bytes.decode("utf-8", errors="replace")

    return truncated + marker
