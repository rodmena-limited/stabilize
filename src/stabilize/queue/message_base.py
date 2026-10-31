"""The message base classes: delivery metadata, then the workflow, stage and task levels."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


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

    # Error tracking for retries - stores context from previous failures
    last_error: str | None = field(default=None, repr=False)
    last_error_type: str | None = field(default=None, repr=False)

    def copy_with_attempts(self, attempts: int) -> Message:
        """Create a copy with updated attempt count."""
        import copy

        new_msg = copy.copy(self)
        new_msg.attempts = attempts
        return new_msg

    def set_error_context(self, error: Exception) -> None:
        """Store error context from a failed attempt.

        This context is preserved across reschedules to help debugging
        and provide visibility into why messages are being retried.
        """
        self.last_error = str(error)
        self.last_error_type = type(error).__name__


@dataclass
class WorkflowLevel(Message):
    """
    Base class for execution-level messages.

    These messages target a specific pipeline execution.
    """

    execution_type: str = "PIPELINE"
    execution_id: str = ""


@dataclass
class StageLevel(WorkflowLevel):
    """
    Base class for stage-level messages.

    These messages target a specific stage within an execution.

    Attributes:
        stage_id: The ID of the stage this message targets
        retry_count: Number of times this message has been re-queued while
                    waiting for upstream stages to complete. Used to prevent infinite loops.
    """

    stage_id: str = ""
    retry_count: int = 0

    @classmethod
    def from_execution_level(cls, msg: WorkflowLevel, stage_id: str) -> StageLevel:
        """Create a stage-level message from an execution-level message."""
        return cls(
            execution_type=msg.execution_type,
            execution_id=msg.execution_id,
            stage_id=stage_id,
        )


@dataclass
class TaskLevel(StageLevel):
    """
    Base class for task-level messages.

    These messages target a specific task within a stage.
    """

    task_id: str = ""
    generation: int | None = None

    @classmethod
    def from_stage_level(cls, msg: StageLevel, task_id: str) -> TaskLevel:
        """Create a task-level message from a stage-level message."""
        return cls(
            execution_type=msg.execution_type,
            execution_id=msg.execution_id,
            stage_id=msg.stage_id,
            task_id=task_id,
        )
