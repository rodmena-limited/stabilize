"""Task-level errors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.errors.base import StabilizeError

if TYPE_CHECKING:
    from stabilize.error_codes import ErrorCode


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
