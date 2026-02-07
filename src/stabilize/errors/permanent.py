"""Permanent (non-retryable) errors and system errors."""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.errors.base import StabilizeError

if TYPE_CHECKING:
    from stabilize.error_codes import ErrorCode


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

    Raised when the system cannot recover a workflow after restart.
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

    Raised during initialization when configuration is invalid.
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
