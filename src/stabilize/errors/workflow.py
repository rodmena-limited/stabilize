"""Workflow, stage, and queue errors."""

from __future__ import annotations

from stabilize.errors.base import StabilizeError


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
