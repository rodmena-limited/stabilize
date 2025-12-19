from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
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

@dataclass
class Message:
    """
    Base class for all queue messages.

    Each message includes metadata for tracking and debugging.
    """
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

@dataclass
class WorkflowLevel(Message):
    """
    Base class for execution-level messages.

    These messages target a specific pipeline execution.
    """
    execution_type: str = 'PIPELINE'
    execution_id: str = ''

@dataclass
class StartWorkflow(WorkflowLevel):
    """
    Message to start a pipeline execution.

    Triggers the beginning of pipeline execution, starting initial stages.
    """

@dataclass
class CompleteWorkflow(WorkflowLevel):
    """
    Message to complete a pipeline execution.

    Sent when all stages have completed or execution should be finalized.
    """

@dataclass
class CancelWorkflow(WorkflowLevel):
    """
    Message to cancel a pipeline execution.

    Marks the execution as canceled and stops all running stages.
    """
    user: str = ''
    reason: str = ''

@dataclass
class StartWaitingWorkflows(Message):
    """
    Message to start any queued/waiting executions for a pipeline config.

    Sent after an execution completes when concurrent execution limits are enabled.
    """
    pipeline_config_id: str = ''
    purge_queue: bool = False

@dataclass
class StageLevel(WorkflowLevel):
    """
    Base class for stage-level messages.

    These messages target a specific stage within an execution.
    """
    stage_id: str = ''

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

@dataclass
class CompleteStage(StageLevel):
    """
    Message to complete a stage.

    Determines stage status, plans after stages, and triggers downstream.
    """

@dataclass
class SkipStage(StageLevel):
    """
    Message to skip a stage.

    Sets stage status to SKIPPED and triggers downstream stages.
    """

@dataclass
class CancelStage(StageLevel):
    """
    Message to cancel a stage.

    Cancels any running tasks and marks stage as canceled.
    """

@dataclass
class RestartStage(StageLevel):
    """
    Message to restart a stage.

    Resets stage status and re-executes from the beginning.
    """

@dataclass
class ResumeStage(StageLevel):
    """
    Message to resume a paused stage.

    Continues execution from where it was paused.
    """

@dataclass
class ContinueParentStage(StageLevel):
    """
    Message to continue parent stage after synthetic stage completes.

    Sent when a synthetic stage completes to notify its parent.
    """
    phase: SyntheticStageOwner = SyntheticStageOwner.STAGE_AFTER

@dataclass
class TaskLevel(StageLevel):
    """
    Base class for task-level messages.

    These messages target a specific task within a stage.
    """
    task_id: str = ''

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
