"""Transient (retryable) errors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.errors.base import StabilizeError

if TYPE_CHECKING:
    from stabilize.error_codes import ErrorCode


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
        super().__init__(message, code=code, cause=cause, error_code=error_code)
        self.retry_after = retry_after
        self.context_update = context_update or {}


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
