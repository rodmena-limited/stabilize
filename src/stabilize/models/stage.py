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
from enum import Enum
from typing import TYPE_CHECKING, Any

from stabilize.models.status import (
    CONTINUABLE_STATUSES,
    WorkflowStatus,
)
from stabilize.models.task import TaskExecution

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow


def _generate_stage_id() -> str:
    """Generate a unique stage ID using ULID."""
    from ulid import ULID

    return str(ULID())


class SyntheticStageOwner(Enum):
    """
    Indicates the relationship of a synthetic stage to its parent.

    STAGE_BEFORE: Runs before the parent's tasks
    STAGE_AFTER: Runs after the parent completes
    """

    STAGE_BEFORE = "STAGE_BEFORE"
    STAGE_AFTER = "STAGE_AFTER"


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

    # Back-reference to parent execution (set after construction)
    # Can be weakref (default) or strong ref (for standalone stages)
    _execution: weakref.ReferenceType[Workflow] | Workflow | None = field(default=None, repr=False)

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

    # ========== DAG Navigation Methods ==========

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
        Returns empty list if stage is not attached to an execution.
        """
        if not self.has_execution():
            return []
        return [stage for stage in self.execution.stages if stage.ref_id in self.requisite_stage_ref_ids]

    def downstream_stages(self) -> list[StageExecution]:
        """
        Get all stages directly downstream of this stage.

        Returns stages that have this stage's ref_id in their requisite_stage_ref_ids.
        Returns empty list if stage is not attached to an execution.
        """
        if not self.has_execution():
            return []
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

    # ========== Synthetic Stage Methods ==========

    def synthetic_stages(self) -> list[StageExecution]:
        """Get all synthetic stages (children) of this stage.

        Returns empty list if stage is not attached to an execution.
        """
        if not self.has_execution():
            return []
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
            ValueError: If this is not a synthetic stage or not attached to an execution
        """
        if self.parent_stage_id is None:
            raise ValueError("Not a synthetic stage")
        if not self.has_execution():
            raise ValueError("Stage is not attached to an execution")
        for stage in self.execution.stages:
            if stage.id == self.parent_stage_id:
                return stage
        raise ValueError(f"Parent stage {self.parent_stage_id} not found")

    def is_synthetic(self) -> bool:
        """Check if this is a synthetic stage."""
        return self.parent_stage_id is not None

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

    # ========== Ancestor Traversal ==========

    def ancestors(self) -> list[StageExecution]:
        """
        Get all ancestor stages in dependency order.

        Includes requisite stages and parent stages.
        """
        visited: set[str] = set()
        result: list[StageExecution] = []

        def visit(stage: StageExecution) -> None:
            if stage.id in visited:
                return
            visited.add(stage.id)
            result.append(stage)

            # Visit requisite stages
            for upstream in stage.upstream_stages():
                visit(upstream)

            # Visit parent stage
            if stage.parent_stage_id:
                try:
                    visit(stage.parent())
                except ValueError:
                    pass

        # Start with upstream stages (not self)
        for upstream in self.upstream_stages():
            visit(upstream)
        if self.parent_stage_id:
            try:
                visit(self.parent())
            except ValueError:
                pass

        return result

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
