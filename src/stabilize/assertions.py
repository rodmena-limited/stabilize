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

def assert_context_type(
    stage: StageExecution,
    key: str,
    expected_type: type[T],
    message: str | None = None,
) -> T:
    """
    Assert that a context key exists and has the expected type.

    Args:
        stage: The stage execution
        key: The context key to check
        expected_type: The expected type
        message: Optional custom error message

    Returns:
        The typed value from context

    Raises:
        ContextError: If key is missing or has wrong type

    Example:
        timeout = assert_context_type(stage, "timeout", int, "Timeout must be an integer")
    """
    value = assert_context(stage, key, message)
    if not isinstance(value, expected_type):
        raise ContextError(
            message or f"Context key '{key}' must be {expected_type.__name__}, got {type(value).__name__}",
            key=key,
        )
    return value

def assert_context_in(
    stage: StageExecution,
    key: str,
    allowed_values: list[Any],
    message: str | None = None,
) -> Any:
    """
    Assert that a context value is in a list of allowed values.

    Args:
        stage: The stage execution
        key: The context key to check
        allowed_values: List of valid values
        message: Optional custom error message

    Returns:
        The value from context

    Raises:
        ContextError: If key is missing or value not allowed

    Example:
        env = assert_context_in(stage, "env", ["dev", "staging", "prod"])
    """
    value = assert_context(stage, key, message)
    if value not in allowed_values:
        raise ContextError(
            message or f"Context key '{key}' must be one of {allowed_values}, got '{value}'",
            key=key,
        )
    return value

def assert_output(
    stage: StageExecution,
    key: str,
    message: str | None = None,
) -> Any:
    """
    Assert that an output key exists and return its value.

    Args:
        stage: The stage execution
        key: The output key to check
        message: Optional custom error message

    Returns:
        The value from outputs

    Raises:
        OutputError: If key is not in outputs

    Example:
        result = assert_output(stage, "deployment_id", "Deployment ID not found")
    """
    if key not in stage.outputs:
        raise OutputError(
            message or f"Required output key '{key}' is missing",
            key=key,
        )
    return stage.outputs[key]

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
