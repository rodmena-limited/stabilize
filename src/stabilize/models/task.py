"""
TaskExecution model.

A task is the smallest unit of work within a stage. Each stage contains
one or more tasks that execute sequentially.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


def _generate_task_id() -> str:
    """Generate a unique task ID using ULID."""
    import ulid

    return str(ulid.new())


@dataclass
class TaskExecution:
    """
    Represents a single task within a stage execution.

    Tasks are the atomic units of work in a pipeline. They execute sequentially
    within a stage, and each task produces a result that can include:
    - Status updates
    - Context modifications (stage-scoped)
    - Output values (pipeline-scoped)

    Attributes:
        id: Unique identifier for this task execution
        name: Human-readable name for this task
        implementing_class: Fully qualified class name of the task implementation
        status: Current execution status
        start_time: Epoch milliseconds when task started
        end_time: Epoch milliseconds when task completed
        stage_start: True if this is the first task in the stage
        stage_end: True if this is the last task in the stage
        loop_start: True if this task starts a loop
        loop_end: True if this task ends a loop
        task_exception_details: Exception information if task failed
    """

    id: str = field(default_factory=_generate_task_id)
    name: str = ""
    implementing_class: str = ""
    status: WorkflowStatus = WorkflowStatus.NOT_STARTED
    start_time: int | None = None
    end_time: int | None = None
    stage_start: bool = False
    stage_end: bool = False
    loop_start: bool = False
    loop_end: bool = False
    task_exception_details: dict[str, Any] = field(default_factory=dict)

    # Back-reference to parent stage (set after construction)
    _stage: StageExecution | None = field(default=None, repr=False)

    @property
    def stage(self) -> StageExecution | None:
        """Get the parent stage for this task."""
        return self._stage

    @stage.setter
    def stage(self, value: StageExecution) -> None:
        """Set the parent stage for this task."""
        self._stage = value

    @property
    def is_stage_start(self) -> bool:
        """Check if this task starts the stage."""
        return self.stage_start

    @property
    def is_stage_end(self) -> bool:
        """Check if this task ends the stage."""
        return self.stage_end

    @property
    def is_loop_start(self) -> bool:
        """Check if this task starts a loop."""
        return self.loop_start

    @property
    def is_loop_end(self) -> bool:
        """Check if this task ends a loop."""
        return self.loop_end

    def set_exception_details(self, exception: dict[str, Any]) -> None:
        """Store exception details for this task."""
        self.task_exception_details["exception"] = exception

    @classmethod
    def create(
        cls,
        name: str,
        implementing_class: str,
        stage_start: bool = False,
        stage_end: bool = False,
    ) -> TaskExecution:
        """
        Factory method to create a new task execution.

        Args:
            name: Human-readable task name
            implementing_class: Class name or callable reference for task
            stage_start: Whether this is the first task
            stage_end: Whether this is the last task

        Returns:
            A new TaskExecution instance
        """
        return cls(
            name=name,
            implementing_class=implementing_class,
            stage_start=stage_start,
            stage_end=stage_end,
        )
