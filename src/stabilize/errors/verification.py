"""Verification errors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.errors.base import StabilizeError
from stabilize.errors.transient import TransientError

if TYPE_CHECKING:
    from stabilize.error_codes import ErrorCode


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
