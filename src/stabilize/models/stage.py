from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any
from stabilize.models.status import (
    CONTINUABLE_STATUSES,
    WorkflowStatus,
)
from stabilize.models.task import TaskExecution

def _generate_stage_id() -> str:
    """Generate a unique stage ID using ULID."""
    import ulid

    return str(ulid.new())

class SyntheticStageOwner(Enum):
    """
    Indicates the relationship of a synthetic stage to its parent.

    STAGE_BEFORE: Runs before the parent's tasks
    STAGE_AFTER: Runs after the parent completes
    """
    STAGE_BEFORE = 'STAGE_BEFORE'
    STAGE_AFTER = 'STAGE_AFTER'

@dataclass
class StageExecution:
    """
    Represents a stage execution within a pipeline.

    The DAG structure is encoded in requisite_stage_ref_ids:
    - Empty set = initial stage (no dependencies)
    - Single ref_id = sequential dependency
    - Multiple ref_ids = join point (waits for all)

    Attributes:
        id: Unique identifier (ULID)
        ref_id: Reference identifier used for DAG relationships
        type: Stage type (e.g., "deploy", "bake", "wait")
        name: Human-readable stage name
        status: Current execution status
        context: Input parameters and runtime state (stage-scoped)
        outputs: Values available to downstream stages (pipeline-scoped)
        tasks: List of tasks to execute in this stage
        requisite_stage_ref_ids: Set of ref_ids this stage depends on (DAG edges)
        parent_stage_id: Parent stage ID for synthetic stages
        synthetic_stage_owner: STAGE_BEFORE or STAGE_AFTER for synthetic stages
        start_time: Epoch milliseconds when stage started
        end_time: Epoch milliseconds when stage completed
        start_time_expiry: If stage not started by this time, skip it
        scheduled_time: When stage is scheduled to execute
    """
    id: str = field(default_factory=_generate_stage_id)
    ref_id: str = ''
    type: str = ''
    name: str = ''
    status: WorkflowStatus = WorkflowStatus.NOT_STARTED
    context: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)
    tasks: list[TaskExecution] = field(default_factory=list)
    requisite_stage_ref_ids: set[str] = field(default_factory=set)
    parent_stage_id: str | None = None
    synthetic_stage_owner: SyntheticStageOwner | None = None
    start_time: int | None = None
    end_time: int | None = None
    start_time_expiry: int | None = None
    scheduled_time: int | None = None
    _execution: Workflow | None = field(default=None, repr=False)

    def execution(self) -> Workflow:
        """Get the parent pipeline execution."""
        if self._execution is None:
            raise ValueError("Stage is not attached to an execution")
        return self._execution

    def execution(self, value: Workflow) -> None:
        """Set the parent pipeline execution."""
        self._execution = value

    def has_execution(self) -> bool:
        """Check if this stage is attached to an execution."""
        return self._execution is not None

    def is_initial(self) -> bool:
        """Check if this is an initial stage (no dependencies)."""
        return len(self.requisite_stage_ref_ids) == 0

    def is_join(self) -> bool:
        """
        Check if this is a join point (multiple dependencies).

        A join point waits for multiple upstream stages to complete.
        """
        return len(self.requisite_stage_ref_ids) > 1

    def upstream_stages(self) -> list[StageExecution]:
        """
        Get all stages directly upstream of this stage.

        Returns stages whose ref_id is in this stage's requisite_stage_ref_ids.
        """
        return [stage for stage in self.execution.stages if stage.ref_id in self.requisite_stage_ref_ids]

    def downstream_stages(self) -> list[StageExecution]:
        """
        Get all stages directly downstream of this stage.

        Returns stages that have this stage's ref_id in their requisite_stage_ref_ids.
        """
        return [stage for stage in self.execution.stages if self.ref_id in stage.requisite_stage_ref_ids]

    def all_upstream_stages_complete(self) -> bool:
        """
        Check if all upstream stages have completed successfully.

        Returns True if all upstream stages have status in CONTINUABLE_STATUSES
        (SUCCEEDED, FAILED_CONTINUE, or SKIPPED).
        """
        return all(stage.status in CONTINUABLE_STATUSES for stage in self.upstream_stages())

    def any_upstream_stages_failed(self) -> bool:
        """
        Check if any upstream stages have failed with a halt status.

        Returns True if any upstream stage has TERMINAL, STOPPED, or CANCELED status.
        """
        halt_statuses = {
            WorkflowStatus.TERMINAL,
            WorkflowStatus.STOPPED,
            WorkflowStatus.CANCELED,
        }
        for upstream in self.upstream_stages():
            if upstream.status in halt_statuses:
                return True
            # Check recursively for NOT_STARTED stages
            if upstream.status == WorkflowStatus.NOT_STARTED and upstream.any_upstream_stages_failed():
                return True
        return False

    def synthetic_stages(self) -> list[StageExecution]:
        """Get all synthetic stages (children) of this stage."""
        return [stage for stage in self.execution.stages if stage.parent_stage_id == self.id]

    def before_stages(self) -> list[StageExecution]:
        """Get synthetic stages that run before this stage's tasks."""
        return [
            stage
            for stage in self.synthetic_stages()
            if stage.synthetic_stage_owner == SyntheticStageOwner.STAGE_BEFORE
        ]

    def after_stages(self) -> list[StageExecution]:
        """Get synthetic stages that run after this stage completes."""
        return [
            stage for stage in self.synthetic_stages() if stage.synthetic_stage_owner == SyntheticStageOwner.STAGE_AFTER
        ]

    def first_before_stages(self) -> list[StageExecution]:
        """Get initial before stages (those with no dependencies among before stages)."""
        return [stage for stage in self.before_stages() if stage.is_initial()]

    def first_after_stages(self) -> list[StageExecution]:
        """Get initial after stages (those with no dependencies among after stages)."""
        return [stage for stage in self.after_stages() if stage.is_initial()]

    def parent(self) -> StageExecution:
        """
        Get the parent stage for this synthetic stage.

        Raises:
            ValueError: If this is not a synthetic stage
        """
        if self.parent_stage_id is None:
            raise ValueError("Not a synthetic stage")
        for stage in self.execution.stages:
            if stage.id == self.parent_stage_id:
                return stage
        raise ValueError(f"Parent stage {self.parent_stage_id} not found")

    def is_synthetic(self) -> bool:
        """Check if this is a synthetic stage."""
        return self.parent_stage_id is not None

    def first_task(self) -> TaskExecution | None:
        """Get the first task in this stage."""
        return self.tasks[0] if self.tasks else None
