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

__version__ = "0.10.4"

# Assertion helpers
from stabilize.assertions import (
    ConfigError,
    ContextError,
    OutputError,
    PreconditionError,
    StabilizeError,
    StabilizeExpectedError,
    StabilizeFatalError,
    StageNotReadyError,
    VerificationError,
    assert_config,
    assert_context,
    assert_context_in,
    assert_context_type,
    assert_no_upstream_failures,
    assert_non_empty,
    assert_not_none,
    assert_output,
    assert_output_type,
    assert_stage_ready,
    assert_true,
    assert_verified,
)

# Structured conditions
from stabilize.conditions import (
    Condition,
    ConditionReason,
    ConditionSet,
    ConditionType,
)

# Configuration validation
from stabilize.config_validation import (
    SchemaValidator,
    ValidationError,
    is_valid,
    validate_context,
    validate_outputs,
)

# Context helpers
from stabilize.context.stage_context import StageContext

# Error hierarchy
from stabilize.errors import (
    PermanentError,
    RecoveryError,
    TaskError,
    TransientError,
    is_permanent,
    is_transient,
)

# Handlers
from stabilize.handlers import (
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    RunTaskHandler,
    StabilizeHandler,
    StartStageHandler,
    StartTaskHandler,
    StartWorkflowHandler,
)

# Observability (optional - graceful degradation if not installed)
from stabilize.logging import (
    bind_context,
    clear_context,
    configure_logging,
    get_logger,
    stage_logger,
    task_logger,
    unbind_context,
    workflow_logger,
)

# Core models
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow

# Infrastructure
from stabilize.orchestrator import Orchestrator
from stabilize.persistence.postgres import PostgresWorkflowStore
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.persistence.store import WorkflowStore
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.queue import PostgresQueue, Queue
from stabilize.queue.sqlite_queue import SqliteQueue

# Recovery
from stabilize.recovery import (
    RecoveryResult,
    WorkflowRecovery,
    recover_on_startup,
)

# Tasks
from stabilize.tasks.docker import DockerTask
from stabilize.tasks.highway import HighwayTask
from stabilize.tasks.http import HTTPTask
from stabilize.tasks.interface import RetryableTask, Task
from stabilize.tasks.python import PythonTask
from stabilize.tasks.registry import TaskRegistry
from stabilize.tasks.result import TaskResult
from stabilize.tasks.shell import ShellTask
from stabilize.tasks.ssh import SSHTask
from stabilize.tracing import (
    add_event,
    configure_tracing,
    get_tracer,
    set_attribute,
    trace_message_processing,
    trace_operation,
    trace_stage,
    trace_task,
    trace_workflow,
)

# Verification system
from stabilize.verification import (
    CallableVerifier,
    OutputVerifier,
    Verifier,
    VerifyResult,
    VerifyStatus,
)

__all__ = [
    # Core models
    "WorkflowStatus",
    "Workflow",
    "StageExecution",
    "TaskExecution",
    # Infrastructure
    "Orchestrator",
    "QueueProcessor",
    "Queue",
    "PostgresQueue",
    "SqliteQueue",
    "WorkflowStore",
    "PostgresWorkflowStore",
    "SqliteWorkflowStore",
    # Handlers
    "StabilizeHandler",
    "StartWorkflowHandler",
    "StartStageHandler",
    "StartTaskHandler",
    "RunTaskHandler",
    "CompleteTaskHandler",
    "CompleteStageHandler",
    "CompleteWorkflowHandler",
    # Tasks
    "Task",
    "RetryableTask",
    "TaskResult",
    "TaskRegistry",
    "ShellTask",
    "HTTPTask",
    "DockerTask",
    "SSHTask",
    "HighwayTask",
    "PythonTask",
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
    # Context helpers
    "StageContext",
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
    # Observability
    "configure_logging",
    "get_logger",
    "bind_context",
    "unbind_context",
    "clear_context",
    "workflow_logger",
    "stage_logger",
    "task_logger",
    "configure_tracing",
    "get_tracer",
    "trace_operation",
    "trace_workflow",
    "trace_stage",
    "trace_task",
    "trace_message_processing",
    "add_event",
    "set_attribute",
    # Error hierarchy
    "TransientError",
    "PermanentError",
    "TaskError",
    "RecoveryError",
    "is_transient",
    "is_permanent",
    # Recovery
    "WorkflowRecovery",
    "RecoveryResult",
    "recover_on_startup",
]
