"""Common validation schemas for Stabilize."""

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
