"""
Assertion helpers for validating preconditions and results.

This module provides assertion utilities that raise descriptive exceptions
when conditions are not met. These are useful for:
- Validating task inputs before processing
- Checking configuration values
- Ensuring stage prerequisites are met
- Verifying outputs after task execution

Example:
    from stabilize.assertions import assert_context, assert_output

    class MyTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            # Validate required inputs
            assert_context(stage, "api_key", "API key is required")
            assert_context_type(stage, "timeout", int, "Timeout must be an integer")

            # Do work...

            return TaskResult.success(outputs={"result": "done"})
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from stabilize.exceptions import (
    ConfigError,
    ContextError,
    OutputError,
    PreconditionError,
    StabilizeError,
    StabilizeExpectedError,
    StabilizeFatalError,
    StageNotReadyError,
    VerificationError,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

T = TypeVar("T")

# Re-export exceptions for backward compatibility
__all__ = [
    "StabilizeError",
    "StabilizeFatalError",
    "StabilizeExpectedError",
    "PreconditionError",
    "ContextError",
    "OutputError",
    "ConfigError",
    "VerificationError",
    "StageNotReadyError",
    "assert_true",
    "assert_context",
    "assert_context_type",
    "assert_context_in",
    "assert_output",
    "assert_output_type",
    "assert_stage_ready",
    "assert_no_upstream_failures",
    "assert_config",
    "assert_verified",
    "assert_not_none",
    "assert_non_empty",
]


# =============================================================================
# Assertion Functions
# =============================================================================


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
            message
            or f"Context key '{key}' must be {expected_type.__name__}, got {type(value).__name__}",
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


def assert_output_type(
    stage: StageExecution,
    key: str,
    expected_type: type[T],
    message: str | None = None,
) -> T:
    """
    Assert that an output key exists and has the expected type.

    Args:
        stage: The stage execution
        key: The output key to check
        expected_type: The expected type
        message: Optional custom error message

    Returns:
        The typed value from outputs

    Raises:
        OutputError: If key is missing or has wrong type

    Example:
        count = assert_output_type(stage, "item_count", int)
    """
    value = assert_output(stage, key, message)
    if not isinstance(value, expected_type):
        raise OutputError(
            message
            or f"Output key '{key}' must be {expected_type.__name__}, got {type(value).__name__}",
            key=key,
        )
    return value


def assert_stage_ready(
    stage: StageExecution,
    message: str | None = None,
) -> None:
    """
    Assert that all upstream stages have completed successfully.

    Args:
        stage: The stage execution to check
        message: Optional custom error message

    Raises:
        StageNotReadyError: If any upstream stage hasn't completed

    Example:
        assert_stage_ready(stage, "Cannot start: upstream stages incomplete")
    """
    if not stage.all_upstream_stages_complete():
        raise StageNotReadyError(
            message or "Upstream stages have not completed",
            stage_ref_id=stage.ref_id,
        )


def assert_no_upstream_failures(
    stage: StageExecution,
    message: str | None = None,
) -> None:
    """
    Assert that no upstream stages have failed.

    Args:
        stage: The stage execution to check
        message: Optional custom error message

    Raises:
        StageNotReadyError: If any upstream stage has failed

    Example:
        assert_no_upstream_failures(stage)
    """
    if stage.any_upstream_stages_failed():
        raise StageNotReadyError(
            message or "One or more upstream stages have failed",
            stage_ref_id=stage.ref_id,
        )


def assert_config(
    condition: bool,
    message: str,
    field: str | None = None,
) -> None:
    """
    Assert a configuration condition.

    Args:
        condition: The condition to check
        message: Error message if condition is false
        field: Optional field name for context

    Raises:
        ConfigError: If condition is false

    Example:
        assert_config(timeout > 0, "Timeout must be positive", field="timeout")
    """
    if not condition:
        raise ConfigError(message, field=field)


def assert_verified(
    condition: bool,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    """
    Assert a verification condition.

    Args:
        condition: The condition to check
        message: Error message if condition is false
        details: Optional details about the failure

    Raises:
        VerificationError: If condition is false

    Example:
        assert_verified(response.ok, "API returned error", {"status": response.status_code})
    """
    if not condition:
        raise VerificationError(message, details=details)


def assert_not_none(
    value: T | None,
    message: str,
) -> T:
    """
    Assert that a value is not None and return it.

    Args:
        value: The value to check
        message: Error message if value is None

    Returns:
        The non-None value

    Raises:
        PreconditionError: If value is None

    Example:
        user = assert_not_none(get_user(id), f"User {id} not found")
    """
    if value is None:
        raise PreconditionError(message)
    return value


def assert_non_empty(
    value: str | list[Any] | dict[str, Any],
    message: str,
) -> str | list[Any] | dict[str, Any]:
    """
    Assert that a value is not empty.

    Works with strings, lists, and dicts.

    Args:
        value: The value to check
        message: Error message if value is empty

    Returns:
        The non-empty value

    Raises:
        PreconditionError: If value is empty

    Example:
        items = assert_non_empty(stage.context.get("items", []), "Items list is empty")
    """
    if not value:
        raise PreconditionError(message)
    return value
