"""Stabilize - Highway Workflow Engine execution layer.

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

from __future__ import annotations

__version__ = "0.34.4"

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

# Context helpers
from stabilize.context.stage_context import StageContext

# Error hierarchy
from stabilize.errors import (
    PermanentError,
    RecoveryError,
    TaskError,
    TransientError,
    TransientVerificationError,
    is_permanent,
    is_transient,
)

# Handlers
from stabilize.handlers import (
    AddMultiInstanceHandler,
    CancelRegionHandler,
    CancelStageHandler,
    CompleteStageHandler,
    CompleteTaskHandler,
    CompleteWorkflowHandler,
    ContinueParentStageHandler,
    JumpToStageHandler,
    RunTaskHandler,
    SignalStageHandler,
    SkipStageHandler,
    StabilizeHandler,
    StartStageHandler,
    StartTaskHandler,
    StartWaitingWorkflowsHandler,
    StartWorkflowHandler,
)

# Agentic ergonomics (additive, opt-in — see docs/guide)
from stabilize.hitl import (
    ApprovalTask,
    approve,
    get_signal,
    reject,
    send_signal,
)

# Lifecycle management
from stabilize.lifecycle import (
    LifecycleManager,
    get_lifecycle_manager,
    install_shutdown_handlers,
    set_lifecycle_manager,
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
from stabilize.models.multi_instance import MultiInstanceConfig
from stabilize.models.stage import JoinType, SplitType, StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow

# Infrastructure
from stabilize.orchestrator import Orchestrator
from stabilize.persistence.postgres import PostgresWorkflowStore
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.persistence.store import WorkflowStore
from stabilize.persistence.tenancy import current_tenant
from stabilize.queue import PostgresQueue, Queue
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.sqlite import SqliteQueue

# Recovery
from stabilize.recovery import (
    RecoveryResult,
    WorkflowRecovery,
    recover_on_startup,
)
from stabilize.reducers import (
    apply_output_reducers,
    get_reducer,
    register_reducer,
)
from stabilize.resilience.cancellation import (
    CancellationToken,
    TaskCancelledError,
    is_cancellation_requested,
    raise_if_cancellation_requested,
)
from stabilize.streaming import (
    StreamItem,
    WorkflowStream,
    emit_progress,
)

# Tasks
from stabilize.tasks.docker import DockerTask
from stabilize.tasks.highway import HighwayTask
from stabilize.tasks.http import HTTPTask
from stabilize.tasks.interface import RetryableTask, Task
from stabilize.tasks.python import PythonTask
from stabilize.tasks.registry import (
    TaskRegistry,
    get_verifier,
    register_verifier,
    verifier,
)
from stabilize.tasks.result import TaskResult
from stabilize.tasks.shell import ShellTask
from stabilize.tasks.ssh import SSHTask
from stabilize.tasks.sub_workflow import SubWorkflowTask
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

# Configuration validation
from stabilize.validation import (
    SchemaValidator,
    ValidationError,
    is_valid,
    validate_context,
    validate_outputs,
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
    "AddMultiInstanceHandler",
    "ApprovalTask",
    "CallableVerifier",
    "CancelRegionHandler",
    "CancelStageHandler",
    # Cooperative cancellation
    "CancellationToken",
    "CompleteStageHandler",
    "CompleteTaskHandler",
    "CompleteWorkflowHandler",
    # Conditions
    "Condition",
    "ConditionReason",
    "ConditionSet",
    "ConditionType",
    "ConfigError",
    "ContextError",
    "ContinueParentStageHandler",
    "DockerTask",
    "HTTPTask",
    "HighwayTask",
    "JoinType",
    "JumpToStageHandler",
    # Lifecycle management
    "LifecycleManager",
    "MultiInstanceConfig",
    # Infrastructure
    "Orchestrator",
    "OutputError",
    "OutputVerifier",
    "PermanentError",
    "PostgresQueue",
    "PostgresWorkflowStore",
    "PreconditionError",
    "PythonTask",
    "Queue",
    "QueueProcessor",
    "RecoveryError",
    "RecoveryResult",
    "RetryableTask",
    "RunTaskHandler",
    "SSHTask",
    "SchemaValidator",
    "ShellTask",
    "SignalStageHandler",
    "SkipStageHandler",
    "SplitType",
    "SqliteQueue",
    "SqliteWorkflowStore",
    # Assertions
    "StabilizeError",
    "StabilizeExpectedError",
    "StabilizeFatalError",
    # Handlers
    "StabilizeHandler",
    # Context helpers
    "StageContext",
    "StageExecution",
    "StageNotReadyError",
    "StartStageHandler",
    "StartTaskHandler",
    "StartWaitingWorkflowsHandler",
    "StartWorkflowHandler",
    "StreamItem",
    "SubWorkflowTask",
    # Tasks
    "Task",
    "TaskCancelledError",
    "TaskError",
    "TaskExecution",
    "TaskRegistry",
    "TaskResult",
    # Error hierarchy
    "TransientError",
    "TransientVerificationError",
    # Configuration validation
    "ValidationError",
    "VerificationError",
    # Verification
    "Verifier",
    "VerifyResult",
    "VerifyStatus",
    "Workflow",
    # Recovery
    "WorkflowRecovery",
    # Core models
    "WorkflowStatus",
    "WorkflowStore",
    # Agentic ergonomics (streaming / HITL / reducers)
    "WorkflowStream",
    "add_event",
    "apply_output_reducers",
    "approve",
    "assert_config",
    "assert_context",
    "assert_context_in",
    "assert_context_type",
    "assert_no_upstream_failures",
    "assert_non_empty",
    "assert_not_none",
    "assert_output",
    "assert_output_type",
    "assert_stage_ready",
    "assert_true",
    "assert_verified",
    "bind_context",
    "clear_context",
    # Observability
    "configure_logging",
    "configure_tracing",
    "current_tenant",
    "emit_progress",
    "get_lifecycle_manager",
    "get_logger",
    "get_reducer",
    "get_signal",
    "get_tracer",
    "get_verifier",
    "install_shutdown_handlers",
    "is_cancellation_requested",
    "is_permanent",
    "is_transient",
    "is_valid",
    "raise_if_cancellation_requested",
    "recover_on_startup",
    "register_reducer",
    # Verifier registry
    "register_verifier",
    "reject",
    "send_signal",
    "set_attribute",
    "set_lifecycle_manager",
    "stage_logger",
    "task_logger",
    "trace_message_processing",
    "trace_operation",
    "trace_stage",
    "trace_task",
    "trace_workflow",
    "unbind_context",
    "validate_context",
    "validate_outputs",
    "verifier",
    "workflow_logger",
]
