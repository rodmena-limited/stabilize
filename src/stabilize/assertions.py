from __future__ import annotations
from typing import TYPE_CHECKING, Any, TypeVar
T = TypeVar("T")

def assert_true(condition: bool, message: str) -> None:
    """
    Assert that a condition is true.

    Args:
        condition: The condition to check
        message: Error message if condition is false

    Raises:
        PreconditionError: If condition is false

    Example:
        assert_true(stage.status == WorkflowStatus.RUNNING, "Stage must be running")
    """
    if not condition:
        raise PreconditionError(message)

def assert_context(
    stage: StageExecution,
    key: str,
    message: str | None = None,
) -> Any:
    """
    Assert that a context key exists and return its value.

    Args:
        stage: The stage execution
        key: The context key to check
        message: Optional custom error message

    Returns:
        The value from context

    Raises:
        ContextError: If key is not in context

    Example:
        api_key = assert_context(stage, "api_key", "API key is required")
    """
    if key not in stage.context:
        raise ContextError(
            message or f"Required context key '{key}' is missing",
            key=key,
        )
    return stage.context[key]

class StabilizeError(Exception):
    """Base exception for Stabilize errors."""

class StabilizeFatalError(StabilizeError):
    """
    Fatal error that should terminate the pipeline.

    Fatal errors indicate unrecoverable situations like invalid configuration
    or programming errors.
    """

class StabilizeExpectedError(StabilizeError):
    """
    Expected error that may allow retry or continuation.

    Expected errors occur during normal operation and may be recoverable.
    """

class PreconditionError(StabilizeExpectedError):
    """
    Error raised when a precondition is not met.

    Precondition errors typically indicate that the stage should be
    retried or that an upstream dependency hasn't completed.
    """
    def __init__(self, message: str, key: str | None = None) -> None:
        super().__init__(message)
        self.key = key

class ContextError(StabilizeFatalError):
    """
    Error raised when required context is missing or invalid.

    Context errors are fatal because they indicate misconfiguration.
    """
    def __init__(self, message: str, key: str | None = None) -> None:
        super().__init__(message)
        self.key = key

class OutputError(StabilizeExpectedError):
    """
    Error raised when expected output is missing or invalid.

    Output errors may be recoverable if the task can be retried.
    """
    def __init__(self, message: str, key: str | None = None) -> None:
        super().__init__(message)
        self.key = key

class ConfigError(StabilizeFatalError):
    """
    Error raised when configuration is invalid.

    Config errors are fatal because they indicate invalid setup.
    """
    def __init__(self, message: str, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field

class VerificationError(StabilizeExpectedError):
    """
    Error raised when verification fails.

    Verification errors may allow retry depending on the verifier.
    """
    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = details or {}

class StageNotReadyError(StabilizeExpectedError):
    """
    Error raised when a stage is not ready for execution.

    This typically means upstream dependencies haven't completed.
    """
    def __init__(self, message: str, stage_ref_id: str | None = None) -> None:
        super().__init__(message)
        self.stage_ref_id = stage_ref_id
