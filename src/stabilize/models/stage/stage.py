"""
StageExecution model.

A stage represents a logical unit of work in a pipeline. Stages can have:
- Prerequisites (other stages that must complete first)
- Tasks (sequential work units)
- Synthetic stages (before/after stages injected by builders)

The DAG structure is represented via requisite_stage_ref_ids, which contains
the ref_ids of all stages this stage depends on.
"""

from __future__ import annotations

import weakref
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from stabilize.models.stage.enums import (
    JoinType,
    SplitType,
    SyntheticStageOwner,
    _generate_stage_id,
)
from stabilize.models.stage.navigation import StageNavigationMixin
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution

if TYPE_CHECKING:
    from stabilize.models.multi_instance import MultiInstanceConfig
    from stabilize.models.snapshot import StageStateSnapshot
    from stabilize.models.workflow import Workflow


@dataclass
class StageExecution(StageNavigationMixin):
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
    ref_id: str = ""
    type: str = ""
    name: str = ""
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
    version: int = 0

    # Finalizer support: cleanup on failure and registered finalizer names
    cleanup_on_failure: bool = False
    finalizer_names: list[str] = field(default_factory=list)

    # ========== Advanced Control-Flow Pattern Fields ==========

    # Join semantics (WCP-7,8,9,28-33,37,38)
    join_type: JoinType = JoinType.AND
    join_threshold: int = 0  # For N_OF_M join: how many upstreams needed

    # Split semantics (WCP-6)
    split_type: SplitType = SplitType.AND
    split_conditions: dict[str, str] = field(default_factory=dict)  # downstream_ref_id -> condition expr

    # Multi-instance configuration (WCP-12-15, 26, 27, 34-36)
    mi_config: MultiInstanceConfig | None = None

    # Deferred choice group (WCP-16)
    deferred_choice_group: str | None = None

    # Milestone gating (WCP-18)
    milestone_ref_id: str | None = None
    milestone_status: str | None = None  # Required status name of milestone stage

    # Mutual exclusion / critical section (WCP-17, 39, 40)
    mutex_key: str | None = None

    # Cancel region (WCP-25)
    cancel_region: str | None = None

    # Back-reference to parent execution (set after construction)
    # Can be weakref (default) or strong ref (for standalone stages)
    _execution: weakref.ReferenceType[Workflow] | Workflow | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.join_type == JoinType.N_OF_M and self.join_threshold < 0:
            raise ValueError(f"join_threshold must be >= 0 for N_OF_M join, got {self.join_threshold}")

    # ========== Phase-Version State Tracking ==========

    @property
    def phase_version(self) -> tuple[str, int]:
        """Return (status_name, version) for optimistic locking with phase check.

        This combines the current status and version number into a single
        tuple for phase-aware optimistic locking. When updating a stage,
        you can pass this to ensure both the version AND the expected
        status match before the update proceeds.

        Returns:
            Tuple of (status_name, version) for comparison.

        Example:
            expected = stage.phase_version
            # ... some operation ...
            store.update_status(stage, expected_phase=expected[0])
        """
        return (self.status.name, self.version)

    def state_snapshot(self) -> StageStateSnapshot:
        """Return frozen copy of current state for read-only use.

        Creates an immutable snapshot of the current stage state that
        can be safely passed to external systems, cached, or logged
        without risk of mutation.

        Returns:
            Frozen StageStateSnapshot with current state.
        """
        from stabilize.models.snapshot import StageStateSnapshot, _freeze_dict

        return StageStateSnapshot(
            id=self.id,
            ref_id=self.ref_id,
            status=self.status,
            version=self.version,
            context=_freeze_dict(self.context),
            outputs=_freeze_dict(self.outputs),
            start_time=self.start_time,
            end_time=self.end_time,
            parent_stage_id=self.parent_stage_id,
        )

    @property
    def execution(self) -> Workflow:
        """Get the parent pipeline execution."""
        if self._execution is None:
            raise ValueError("Stage is not attached to an execution")

        if isinstance(self._execution, weakref.ReferenceType):
            exe = self._execution()
            if exe is None:
                raise ValueError("Execution has been garbage collected")
            return exe

        # Strong reference
        return self._execution

    @execution.setter
    def execution(self, value: Workflow) -> None:
        """Set the parent pipeline execution (weakref by default)."""
        self._execution = weakref.ref(value)

    def set_execution_strong(self, value: Workflow) -> None:
        """Set the parent pipeline execution as a strong reference."""
        self._execution = value

    def has_execution(self) -> bool:
        """Check if this stage is attached to an execution."""
        if self._execution is None:
            return False
        if isinstance(self._execution, weakref.ReferenceType):
            return self._execution() is not None
        return True

    def cleanup(self) -> None:
        """Explicitly break circular references."""
        self._execution = None
        for task in self.tasks:
            task.cleanup()

    # ========== Task Methods ==========

    def first_task(self) -> TaskExecution | None:
        """Get the first task in this stage."""
        return self.tasks[0] if self.tasks else None

    def next_task(self, task: TaskExecution) -> TaskExecution | None:
        """Get the task that follows the given task."""
        if task.is_stage_end:
            return None
        try:
            index = self.tasks.index(task)
            return self.tasks[index + 1]
        except (ValueError, IndexError):
            return None

    def has_tasks(self) -> bool:
        """Check if this stage has any tasks."""
        return len(self.tasks) > 0

    # ========== Status Methods ==========

    def determine_status(self) -> WorkflowStatus:
        """Determine the stage status based on before-stages, tasks, and after-stages.

        Status priority (highest to lowest):
        1. TERMINAL/STOPPED/CANCELED - halt conditions
        2. PAUSED/BUFFERED/SUSPENDED - waiting conditions
        3. RUNNING - in progress
        4. FAILED_CONTINUE - completed with non-fatal failure
        5. SUCCEEDED/SKIPPED - completed successfully

        After-stages ARE included in status determination to ensure the stage
        doesn't report SUCCEEDED while after-stages are still running.
        """
        # Collect statuses from all components
        before_stage_statuses = [s.status for s in self.before_stages()]
        task_statuses = [t.status for t in self.tasks]
        after_stage_statuses = [s.status for s in self.after_stages()]

        # Core statuses (before-stages + tasks) determine the main outcome
        core_statuses = before_stage_statuses + task_statuses

        if not core_statuses:
            # No tasks and no before-stages: if the stage is already RUNNING
            # (claimed by StartStageHandler), treat as a no-op success rather
            # than NOT_STARTED which would incorrectly terminate the workflow.
            if self.status == WorkflowStatus.RUNNING:
                if after_stage_statuses:
                    if any(s in {WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING} for s in after_stage_statuses):
                        return WorkflowStatus.RUNNING
                    if WorkflowStatus.TERMINAL in after_stage_statuses:
                        return WorkflowStatus.TERMINAL
                return WorkflowStatus.SUCCEEDED
            return WorkflowStatus.NOT_STARTED

        # Check halt conditions first (highest priority)
        if WorkflowStatus.TERMINAL in core_statuses:
            return self.failure_status()
        if WorkflowStatus.STOPPED in core_statuses:
            return WorkflowStatus.STOPPED
        if WorkflowStatus.CANCELED in core_statuses:
            return WorkflowStatus.CANCELED

        # Check waiting conditions
        if WorkflowStatus.PAUSED in core_statuses:
            return WorkflowStatus.PAUSED
        if WorkflowStatus.BUFFERED in core_statuses:
            return WorkflowStatus.BUFFERED
        if WorkflowStatus.SUSPENDED in core_statuses:
            return WorkflowStatus.SUSPENDED

        # Check if core work is still in progress
        incomplete_statuses = {WorkflowStatus.NOT_STARTED, WorkflowStatus.RUNNING}
        if any(s in incomplete_statuses for s in core_statuses):
            return WorkflowStatus.RUNNING

        # Core work is complete - check if it succeeded or failed with continue
        core_has_failure = WorkflowStatus.FAILED_CONTINUE in core_statuses
        core_all_done = all(
            s in {WorkflowStatus.SUCCEEDED, WorkflowStatus.SKIPPED, WorkflowStatus.FAILED_CONTINUE}
            for s in core_statuses
        )

        if not core_all_done:
            # Unexpected status in core - treat as running
            return WorkflowStatus.RUNNING

        # Core work is done - now check after-stages
        if after_stage_statuses:
            # Check if after-stages have halted
            if WorkflowStatus.TERMINAL in after_stage_statuses:
                return WorkflowStatus.TERMINAL
            if WorkflowStatus.STOPPED in after_stage_statuses:
                return WorkflowStatus.STOPPED
            if WorkflowStatus.CANCELED in after_stage_statuses:
                return WorkflowStatus.CANCELED

            # Check if after-stages are still in progress
            if any(s in incomplete_statuses for s in after_stage_statuses):
                return WorkflowStatus.RUNNING

        # All work complete - return final status
        if core_has_failure:
            return WorkflowStatus.FAILED_CONTINUE
        return WorkflowStatus.SUCCEEDED

    def failure_status(self, default: WorkflowStatus = WorkflowStatus.TERMINAL) -> WorkflowStatus:
        """Get the appropriate failure status based on stage configuration."""
        if self.continue_pipeline_on_failure:
            return WorkflowStatus.FAILED_CONTINUE
        if self.should_fail_pipeline():
            return default
        return WorkflowStatus.STOPPED

    @property
    def continue_pipeline_on_failure(self) -> bool:
        """Check if pipeline should continue on stage failure."""
        return bool(self.context.get("continuePipelineOnFailure", False))

    def should_fail_pipeline(self) -> bool:
        """Check if stage failure should fail the pipeline."""
        return bool(self.context.get("failPipeline", True))

    @property
    def allow_sibling_stages_to_continue_on_failure(self) -> bool:
        """Check if sibling stages can continue on this stage's failure."""
        return bool(self.context.get("allowSiblingStagesToContinueOnFailure", False))

    # ========== Factory Methods ==========

    @classmethod
    def create(
        cls,
        type: str,
        name: str,
        ref_id: str,
        context: dict[str, Any] | None = None,
        requisite_stage_ref_ids: set[str] | None = None,
    ) -> StageExecution:
        """
        Factory method to create a new stage execution.

        Args:
            type: Stage type
            name: Human-readable name
            ref_id: Reference ID for DAG relationships
            context: Initial context/parameters
            requisite_stage_ref_ids: Dependencies (empty = initial stage)

        Returns:
            A new StageExecution instance
        """
        return cls(
            type=type,
            name=name,
            ref_id=ref_id,
            context=context or {},
            requisite_stage_ref_ids=requisite_stage_ref_ids or set(),
        )

    @classmethod
    def create_synthetic(
        cls,
        type: str,
        name: str,
        parent: StageExecution,
        owner: SyntheticStageOwner,
        context: dict[str, Any] | None = None,
    ) -> StageExecution:
        """
        Factory method to create a synthetic stage.

        Args:
            type: Stage type
            name: Human-readable name
            parent: Parent stage
            owner: STAGE_BEFORE or STAGE_AFTER
            context: Initial context/parameters

        Returns:
            A new synthetic StageExecution
        """
        from ulid import ULID

        stage = cls(
            type=type,
            name=name,
            ref_id=str(ULID()),  # Synthetic stages get unique ref_ids
            context=context or {},
            parent_stage_id=parent.id,
            synthetic_stage_owner=owner,
        )
        if parent.has_execution():
            stage.execution = parent.execution
        return stage
