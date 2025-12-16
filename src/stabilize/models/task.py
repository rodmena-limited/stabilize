from __future__ import annotations
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from stabilize.models.status import WorkflowStatus

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
    name: str = ''
    implementing_class: str = ''
    status: WorkflowStatus = WorkflowStatus.NOT_STARTED
    start_time: int | None = None
    end_time: int | None = None
    stage_start: bool = False
    stage_end: bool = False
    loop_start: bool = False
    loop_end: bool = False
    task_exception_details: dict[str, Any] = field(default_factory=dict)
    _stage: StageExecution | None = field(default=None, repr=False)

    def stage(self) -> StageExecution | None:
        """Get the parent stage for this task."""
        return self._stage

    def stage(self, value: StageExecution) -> None:
        """Set the parent stage for this task."""
        self._stage = value

    def is_stage_start(self) -> bool:
        """Check if this task starts the stage."""
        return self.stage_start
