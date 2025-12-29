"""
Configuration validation for stage contexts and workflow configurations.

This module provides JSON Schema-based validation for:
- Stage context dictionaries
- Workflow configurations
- Task parameters

Validation is optional and non-breaking - stages without schemas work as before.

Example:
    from stabilize.config_validation import validate_context, ValidationError

    # Define a schema for your task's expected context
    DEPLOY_SCHEMA = {
        "type": "object",
        "required": ["cluster", "image"],
        "properties": {
            "cluster": {"type": "string", "minLength": 1},
            "image": {"type": "string", "pattern": "^[a-z0-9./-]+:[a-z0-9.-]+$"},
            "replicas": {"type": "integer", "minimum": 1, "default": 1},
            "timeout": {"type": "integer", "minimum": 0, "default": 300},
        },
        "additionalProperties": True,
    }

    # Validate in your task
    class DeployTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            errors = validate_context(stage.context, DEPLOY_SCHEMA)
            if errors:
                return TaskResult.terminal(f"Invalid config: {errors[0]}")
            ...
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ValidationError:
    """
    A validation error with path and message.

    Attributes:
        path: JSON path to the invalid field (e.g., "timeout" or "servers[0].host")
        message: Description of the validation error
        value: The invalid value (if available)
        constraint: The constraint that was violated (if available)
    """

    path: str
    message: str
    value: Any = None
    constraint: str | None = None

    def __str__(self) -> str:
        if self.path:
            return f"{self.path}: {self.message}"
        return self.message


class SchemaValidator:
    """
    JSON Schema-like validator for configuration dictionaries.

    This is a lightweight implementation that doesn't require jsonschema package.
    Supports common validation patterns:
    - type checking (string, integer, number, boolean, array, object, null)
    - required fields
    - enum values
    - min/max for numbers
    - minLength/maxLength for strings
    - pattern matching for strings
    - minItems/maxItems for arrays
    - items schema for arrays
    - properties for objects
    - additionalProperties control

    Example:
        validator = SchemaValidator({
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": {"type": "string", "minLength": 1},
                "age": {"type": "integer", "minimum": 0},
            },
        })

        errors = validator.validate({"name": "", "age": -1})
        # Returns [ValidationError("name", "must have minimum length 1"),
        #          ValidationError("age", "must be >= 0")]
    """

    TYPE_MAP = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": type(None),
    }

    def __init__(self, schema: dict[str, Any]) -> None:
        """
        Initialize with a JSON Schema.

        Args:
            schema: The JSON Schema dictionary
        """
        self.schema = schema

    def validate(self, data: Any, path: str = "") -> list[ValidationError]:
        """
        Validate data against the schema.

        Args:
            data: The data to validate
            path: Current path in the data structure (for error messages)

        Returns:
            List of validation errors (empty if valid)
        """
        return self._validate_value(data, self.schema, path)

    def _validate_value(
        self,
        value: Any,
        schema: dict[str, Any],
        path: str,
    ) -> list[ValidationError]:
        """Validate a single value against its schema."""
        errors: list[ValidationError] = []

        # Handle null/None
        if value is None:
            if schema.get("type") == "null":
                return []
            # Allow None if not explicitly typed
            if "type" not in schema:
                return []
            errors.append(ValidationError(path, "value cannot be null", value))
            return errors

        # Type validation
        if "type" in schema:
            expected_type = schema["type"]
            if isinstance(expected_type, list):
                # Union type
                valid = False
                for t in expected_type:
                    if self._check_type(value, t):
                        valid = True
                        break
                if not valid:
                    errors.append(
                        ValidationError(
                            path,
                            f"must be one of types: {', '.join(expected_type)}",
                            value,
                            "type",
                        )
                    )
                    return errors
            else:
                if not self._check_type(value, expected_type):
                    errors.append(
                        ValidationError(
                            path,
                            f"must be {expected_type}, got {type(value).__name__}",
                            value,
                            "type",
                        )
                    )
                    return errors

        # Enum validation
        if "enum" in schema:
            if value not in schema["enum"]:
                errors.append(
                    ValidationError(
                        path,
                        f"must be one of: {schema['enum']}",
                        value,
                        "enum",
                    )
                )

        # Const validation
        if "const" in schema:
            if value != schema["const"]:
                errors.append(
                    ValidationError(
                        path,
                        f"must be exactly {schema['const']!r}",
                        value,
                        "const",
                    )
                )

        # String validations
        if isinstance(value, str):
            errors.extend(self._validate_string(value, schema, path))

        # Number validations
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            errors.extend(self._validate_number(value, schema, path))

        # Array validations
        if isinstance(value, list):
            errors.extend(self._validate_array(value, schema, path))

        # Object validations
        if isinstance(value, dict):
            errors.extend(self._validate_object(value, schema, path))

        return errors

    def _check_type(self, value: Any, type_name: str) -> bool:
        """Check if value matches the JSON Schema type."""
        # Special case: integer shouldn't match boolean
        if type_name == "integer" and isinstance(value, bool):
            return False
        expected = self.TYPE_MAP.get(type_name)
        if expected is None:
            return True  # Unknown type, allow
        return isinstance(value, expected)  # type: ignore[arg-type]

    def _validate_string(
        self,
        value: str,
        schema: dict[str, Any],
        path: str,
    ) -> list[ValidationError]:
        """Validate string-specific constraints."""
        errors: list[ValidationError] = []

        if "minLength" in schema and len(value) < schema["minLength"]:
            errors.append(
                ValidationError(
                    path,
                    f"must have minimum length {schema['minLength']}",
                    value,
                    "minLength",
                )
            )

        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errors.append(
                ValidationError(
                    path,
                    f"must have maximum length {schema['maxLength']}",
                    value,
                    "maxLength",
                )
            )

        if "pattern" in schema:
            import re

            if not re.match(schema["pattern"], value):
                errors.append(
                    ValidationError(
                        path,
                        f"must match pattern {schema['pattern']}",
                        value,
                        "pattern",
                    )
                )

        return errors

    def _validate_number(
        self,
        value: int | float,
        schema: dict[str, Any],
        path: str,
    ) -> list[ValidationError]:
        """Validate number-specific constraints."""
        errors: list[ValidationError] = []

        if "minimum" in schema and value < schema["minimum"]:
            errors.append(
                ValidationError(
                    path,
                    f"must be >= {schema['minimum']}",
                    value,
                    "minimum",
                )
            )

        if "maximum" in schema and value > schema["maximum"]:
            errors.append(
                ValidationError(
                    path,
                    f"must be <= {schema['maximum']}",
                    value,
                    "maximum",
                )
            )

        if "exclusiveMinimum" in schema and value <= schema["exclusiveMinimum"]:
            errors.append(
                ValidationError(
                    path,
                    f"must be > {schema['exclusiveMinimum']}",
                    value,
                    "exclusiveMinimum",
                )
            )

        if "exclusiveMaximum" in schema and value >= schema["exclusiveMaximum"]:
            errors.append(
                ValidationError(
                    path,
                    f"must be < {schema['exclusiveMaximum']}",
                    value,
                    "exclusiveMaximum",
                )
            )

        if "multipleOf" in schema and value % schema["multipleOf"] != 0:
            errors.append(
                ValidationError(
                    path,
                    f"must be multiple of {schema['multipleOf']}",
                    value,
                    "multipleOf",
                )
            )

        return errors

    def _validate_array(
        self,
        value: list,
        schema: dict[str, Any],
        path: str,
    ) -> list[ValidationError]:
        """Validate array-specific constraints."""
        errors: list[ValidationError] = []

        if "minItems" in schema and len(value) < schema["minItems"]:
            errors.append(
                ValidationError(
                    path,
                    f"must have at least {schema['minItems']} items",
                    value,
                    "minItems",
                )
            )

        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(
                ValidationError(
                    path,
                    f"must have at most {schema['maxItems']} items",
                    value,
                    "maxItems",
                )
            )

        if "uniqueItems" in schema and schema["uniqueItems"]:
            # Check for duplicates (works for hashable items)
            try:
                if len(value) != len(set(value)):
                    errors.append(
                        ValidationError(
                            path,
                            "must have unique items",
                            value,
                            "uniqueItems",
                        )
                    )
            except TypeError:
                pass  # Non-hashable items, skip check

        # Validate items
        if "items" in schema:
            items_schema = schema["items"]
            for i, item in enumerate(value):
                item_path = f"{path}[{i}]" if path else f"[{i}]"
                errors.extend(self._validate_value(item, items_schema, item_path))

        return errors

    def _validate_object(
        self,
        value: dict,
        schema: dict[str, Any],
        path: str,
    ) -> list[ValidationError]:
        """Validate object-specific constraints."""
        errors: list[ValidationError] = []

        # Required fields
        if "required" in schema:
            for field in schema["required"]:
                if field not in value:
                    field_path = f"{path}.{field}" if path else field
                    errors.append(
                        ValidationError(
                            field_path,
                            "is required",
                            constraint="required",
                        )
                    )

        # Properties
        properties = schema.get("properties", {})
        for field, field_schema in properties.items():
            if field in value:
                field_path = f"{path}.{field}" if path else field
                errors.extend(self._validate_value(value[field], field_schema, field_path))

        # Additional properties
        additional = schema.get("additionalProperties", True)
        if additional is False:
            allowed = set(properties.keys())
            extra = set(value.keys()) - allowed
            if extra:
                for field in extra:
                    field_path = f"{path}.{field}" if path else field
                    errors.append(
                        ValidationError(
                            field_path,
                            "is not an allowed property",
                            constraint="additionalProperties",
                        )
                    )
        elif isinstance(additional, dict):
            # Validate additional properties against schema
            allowed = set(properties.keys())
            for field in value:
                if field not in allowed:
                    field_path = f"{path}.{field}" if path else field
                    errors.extend(self._validate_value(value[field], additional, field_path))

        # Min/max properties
        if "minProperties" in schema and len(value) < schema["minProperties"]:
            errors.append(
                ValidationError(
                    path,
                    f"must have at least {schema['minProperties']} properties",
                    constraint="minProperties",
                )
            )

        if "maxProperties" in schema and len(value) > schema["maxProperties"]:
            errors.append(
                ValidationError(
                    path,
                    f"must have at most {schema['maxProperties']} properties",
                    constraint="maxProperties",
                )
            )

        return errors


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


# ============================================================================
# Common Schemas
# ============================================================================

# Schema for shell task context
SHELL_TASK_SCHEMA = {
    "type": "object",
    "required": ["command"],
    "properties": {
        "command": {"type": "string", "minLength": 1},
        "timeout": {"type": "integer", "minimum": 0},
        "cwd": {"type": "string"},
        "env": {"type": "object", "additionalProperties": {"type": "string"}},
        "shell": {"type": ["boolean", "string"]},
        "stdin": {"type": "string"},
        "max_output_size": {"type": "integer", "minimum": 0},
        "expected_codes": {"type": "array", "items": {"type": "integer"}},
        "secrets": {"type": "array", "items": {"type": "string"}},
        "binary": {"type": "boolean"},
        "continue_on_failure": {"type": "boolean"},
    },
}

# Schema for wait task context
WAIT_TASK_SCHEMA = {
    "type": "object",
    "required": ["waitTime"],
    "properties": {
        "waitTime": {"type": "integer", "minimum": 0},
    },
}

# Base stage schema (without specific task requirements)
BASE_STAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "failPipeline": {"type": "boolean"},
        "continuePipelineOnFailure": {"type": "boolean"},
        "allowSiblingStagesToContinueOnFailure": {"type": "boolean"},
        "stageTimeoutMs": {"type": "integer", "minimum": 0},
    },
}
