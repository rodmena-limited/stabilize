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
