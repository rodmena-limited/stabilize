"""Validation helper functions."""

from __future__ import annotations

from typing import Any

from stabilize.validation.errors import ValidationError
from stabilize.validation.validator import SchemaValidator


def validate_context(
    context: dict[str, Any],
    schema: dict[str, Any],
) -> list[ValidationError]:
    """
    Validate a stage context against a schema.

    Args:
        context: The stage context dictionary
        schema: JSON Schema dictionary

    Returns:
        List of validation errors (empty if valid)

    Example:
        errors = validate_context(stage.context, {
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": {"type": "string", "minLength": 1},
                "timeout": {"type": "integer", "minimum": 0},
            },
        })
        if errors:
            return TaskResult.terminal(f"Invalid context: {errors[0]}")
    """
    validator = SchemaValidator(schema)
    return validator.validate(context)


def validate_outputs(
    outputs: dict[str, Any],
    schema: dict[str, Any],
) -> list[ValidationError]:
    """
    Validate stage outputs against a schema.

    Args:
        outputs: The stage outputs dictionary
        schema: JSON Schema dictionary

    Returns:
        List of validation errors (empty if valid)
    """
    validator = SchemaValidator(schema)
    return validator.validate(outputs)


def is_valid(data: Any, schema: dict[str, Any]) -> bool:
    """
    Check if data is valid against a schema.

    Args:
        data: The data to validate
        schema: JSON Schema dictionary

    Returns:
        True if valid, False otherwise
    """
    validator = SchemaValidator(schema)
    return len(validator.validate(data)) == 0
