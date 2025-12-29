from __future__ import annotations
from dataclasses import dataclass
from typing import Any
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
WAIT_TASK_SCHEMA = {
    "type": "object",
    "required": ["waitTime"],
    "properties": {
        "waitTime": {"type": "integer", "minimum": 0},
    },
}
BASE_STAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "failPipeline": {"type": "boolean"},
        "continuePipelineOnFailure": {"type": "boolean"},
        "allowSiblingStagesToContinueOnFailure": {"type": "boolean"},
        "stageTimeoutMs": {"type": "integer", "minimum": 0},
    },
}

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
    TYPE_MAP = {'string': str, 'integer': int, 'number': (int, float), 'boolean': bool, 'array': list, 'object': dict, 'null': type(None)}
    def __init__(self, schema: dict[str, Any]) -> None:
        """
        Initialize with a JSON Schema.

        Args:
            schema: The JSON Schema dictionary
        """
        self.schema = schema
