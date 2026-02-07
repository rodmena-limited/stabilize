"""Base exception hierarchy for Stabilize.

Two-tier exception hierarchy following DBOS patterns:

1. StabilizeBaseException - Base for all errors, not caught by default handlers
2. StabilizeError - Standard errors that can be caught and handled
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
        super().__init__(message)
        if code is not None:
            self.code = code
        self.cause = cause
        if error_code is not None:
            self._error_code = error_code

    @property
    def error_code(self) -> ErrorCode:
        """Get the semantic error code for this exception."""
        if self._error_code is not None:
            return self._error_code
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
