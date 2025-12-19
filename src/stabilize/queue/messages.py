"""
Message types for the queue-based execution engine.

This module defines all message types used in the pipeline execution queue.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus


@dataclass
class Message:
    """
    Base class for all queue messages.

    Each message includes metadata for tracking and debugging.
    """

    # Message metadata
    message_id: str | None = field(default=None, repr=False)
    created_at: datetime = field(default_factory=datetime.now, repr=False)
    attempts: int = field(default=0, repr=False)
    max_attempts: int = field(default=10, repr=False)

    def copy_with_attempts(self, attempts: int) -> Message:
        """Create a copy with updated attempt count."""
        import copy

        new_msg = copy.copy(self)
        new_msg.attempts = attempts
        return new_msg


# ============================================================================
# Execution-level messages
# ============================================================================


@dataclass
class WorkflowLevel(Message):
    """
    Base class for execution-level messages.

    These messages target a specific pipeline execution.
    """

    execution_type: str = "PIPELINE"
    execution_id: str = ""


@dataclass
class StartWorkflow(WorkflowLevel):
    """
    Message to start a pipeline execution.

    Triggers the beginning of pipeline execution, starting initial stages.
    """

    pass


@dataclass
class CompleteWorkflow(WorkflowLevel):
    """
    Message to complete a pipeline execution.

    Sent when all stages have completed or execution should be finalized.
    """

    pass


@dataclass
class CancelWorkflow(WorkflowLevel):
    """
    Message to cancel a pipeline execution.

    Marks the execution as canceled and stops all running stages.
    """

    user: str = ""
    reason: str = ""


@dataclass
class StartWaitingWorkflows(Message):
    """
    Message to start any queued/waiting executions for a pipeline config.

    Sent after an execution completes when concurrent execution limits are enabled.
    """

    pipeline_config_id: str = ""
    purge_queue: bool = False


# ============================================================================
# Stage-level messages
# ============================================================================


@dataclass
class StageLevel(WorkflowLevel):
    """
    Base class for stage-level messages.

    These messages target a specific stage within an execution.
    """

    stage_id: str = ""

    @classmethod
    def from_execution_level(cls, msg: WorkflowLevel, stage_id: str) -> StageLevel:
        """Create a stage-level message from an execution-level message."""
        return cls(
            execution_type=msg.execution_type,
            execution_id=msg.execution_id,
            stage_id=stage_id,
        )


@dataclass
class StartStage(StageLevel):
    """
    Message to start a stage.

    Checks if upstream stages are complete, then plans and starts the stage.
    """

    pass


@dataclass
class CompleteStage(StageLevel):
    """
    Message to complete a stage.

    Determines stage status, plans after stages, and triggers downstream.
    """

    pass


@dataclass
class SkipStage(StageLevel):
    """
    Message to skip a stage.

    Sets stage status to SKIPPED and triggers downstream stages.
    """

    pass


@dataclass
class CancelStage(StageLevel):
    """
    Message to cancel a stage.

    Cancels any running tasks and marks stage as canceled.
    """

    pass


@dataclass
class RestartStage(StageLevel):
    """
    Message to restart a stage.

    Resets stage status and re-executes from the beginning.
    """

    pass


@dataclass
class ResumeStage(StageLevel):
    """
    Message to resume a paused stage.

    Continues execution from where it was paused.
    """

    pass


@dataclass
class ContinueParentStage(StageLevel):
    """
    Message to continue parent stage after synthetic stage completes.

    Sent when a synthetic stage completes to notify its parent.
    """

    phase: SyntheticStageOwner = SyntheticStageOwner.STAGE_AFTER


# ============================================================================
# Task-level messages
# ============================================================================


@dataclass
class TaskLevel(StageLevel):
    """
    Base class for task-level messages.

    These messages target a specific task within a stage.
    """

    task_id: str = ""

    @classmethod
    def from_stage_level(cls, msg: StageLevel, task_id: str) -> TaskLevel:
        """Create a task-level message from a stage-level message."""
        return cls(
            execution_type=msg.execution_type,
            execution_id=msg.execution_id,
            stage_id=msg.stage_id,
            task_id=task_id,
        )


@dataclass
class StartTask(TaskLevel):
    """
    Message to start a task.

    Sets task status to RUNNING and triggers RunTask.
    """

    pass


@dataclass
class RunTask(TaskLevel):
    """
    Message to execute a task.

    Runs the task implementation and handles the result.
    """

    task_type: str = ""


@dataclass
class CompleteTask(TaskLevel):
    """
    Message to complete a task.

    Updates task status and triggers next task or stage completion.
    """

    status: WorkflowStatus = WorkflowStatus.SUCCEEDED
    original_status: WorkflowStatus | None = None


@dataclass
class PauseTask(TaskLevel):
    """
    Message to pause a task.

    Used when execution is paused - task will resume when execution resumes.
    """

    pass


# ============================================================================
# Error messages
# ============================================================================


@dataclass
class InvalidWorkflowId(WorkflowLevel):
    """
    Message indicating an invalid execution ID was referenced.

    Logged and dropped - no further processing.
    """

    pass


@dataclass
class InvalidStageId(StageLevel):
    """
    Message indicating an invalid stage ID was referenced.

    Logged and dropped - no further processing.
    """

    pass


@dataclass
class InvalidTaskId(TaskLevel):
    """
    Message indicating an invalid task ID was referenced.

    Logged and dropped - no further processing.
    """

    pass


@dataclass
class InvalidTaskType(TaskLevel):
    """
    Message indicating an unknown task type was referenced.

    Logged and dropped - no further processing.
    """

    task_type_name: str = ""


# ============================================================================
# Message Registry
# ============================================================================


# Map of message type names to classes for deserialization
MESSAGE_TYPES: dict[str, type[Message]] = {
    "StartWorkflow": StartWorkflow,
    "CompleteWorkflow": CompleteWorkflow,
    "CancelWorkflow": CancelWorkflow,
    "StartWaitingWorkflows": StartWaitingWorkflows,
    "StartStage": StartStage,
    "CompleteStage": CompleteStage,
    "SkipStage": SkipStage,
    "CancelStage": CancelStage,
    "RestartStage": RestartStage,
    "ResumeStage": ResumeStage,
    "ContinueParentStage": ContinueParentStage,
    "StartTask": StartTask,
    "RunTask": RunTask,
    "CompleteTask": CompleteTask,
    "PauseTask": PauseTask,
    "InvalidWorkflowId": InvalidWorkflowId,
    "InvalidStageId": InvalidStageId,
    "InvalidTaskId": InvalidTaskId,
    "InvalidTaskType": InvalidTaskType,
}


def get_message_type_name(message: Message) -> str:
    """Get the type name for a message."""
    return message.__class__.__name__


def create_message_from_dict(type_name: str, data: dict[str, Any]) -> Message:
    """
    Create a message from a dictionary representation.

    Args:
        type_name: The message type name
        data: Dictionary of message fields

    Returns:
        A Message instance

    Raises:
        ValueError: If type_name is unknown
    """
    if type_name not in MESSAGE_TYPES:
        raise ValueError(f"Unknown message type: {type_name}")

    message_class = MESSAGE_TYPES[type_name]
    return message_class(**data)
