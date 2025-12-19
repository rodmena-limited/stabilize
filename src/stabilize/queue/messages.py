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
