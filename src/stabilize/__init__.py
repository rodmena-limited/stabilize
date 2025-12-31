"""
Stabilize - Highway Workflow Engine execution layer.

This package provides a message-driven DAG execution engine for running
workflows with full support for:
- Parallel and sequential stage execution
- Synthetic stages (before/after/onFailure)
- PostgreSQL and SQLite persistence
- Pluggable task system
- Verification phase for validating outputs
- Structured status conditions
- Assertion helpers for clean error handling
- Configuration validation with JSON Schema
"""

__version__ = "0.10.0"

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.tasks.interface import RetryableTask, Task
from stabilize.tasks.result import TaskResult

# Verification system
from stabilize.verification import (
    Verifier,
    VerifyResult,
    VerifyStatus,
    OutputVerifier,
    CallableVerifier,
)

# Structured conditions
from stabilize.conditions import (
    Condition,
    ConditionSet,
    ConditionType,
    ConditionReason,
)

# Assertion helpers
from stabilize.assertions import (
    StabilizeError,
    StabilizeFatalError,
    StabilizeExpectedError,
    PreconditionError,
    ContextError,
    OutputError,
    ConfigError,
    VerificationError,
    StageNotReadyError,
    assert_true,
    assert_context,
    assert_context_type,
    assert_context_in,
    assert_output,
    assert_output_type,
    assert_stage_ready,
    assert_no_upstream_failures,
    assert_config,
    assert_verified,
    assert_not_none,
    assert_non_empty,
)

# Configuration validation
from stabilize.config_validation import (
    ValidationError,
    SchemaValidator,
    validate_context,
    validate_outputs,
    is_valid,
)

__all__ = [
    # Core models
    "WorkflowStatus",
    "Workflow",
    "StageExecution",
    "TaskExecution",
    "TaskResult",
    "Task",
    "RetryableTask",
    # Verification
    "Verifier",
    "VerifyResult",
    "VerifyStatus",
    "OutputVerifier",
    "CallableVerifier",
    # Conditions
    "Condition",
    "ConditionSet",
    "ConditionType",
    "ConditionReason",
    # Assertions
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
    # Configuration validation
    "ValidationError",
    "SchemaValidator",
    "validate_context",
    "validate_outputs",
    "is_valid",
]
