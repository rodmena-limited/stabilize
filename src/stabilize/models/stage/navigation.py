"""
DAG navigation and synthetic stage methods for StageExecution.

This mixin provides methods for traversing the DAG structure, querying
upstream/downstream stages, and managing synthetic stages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.models.stage.enums import SyntheticStageOwner
from stabilize.models.status import CONTINUABLE_STATUSES, WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage.stage import StageExecution
    from stabilize.models.workflow import Workflow


class StageNavigationMixin:
    """Mixin providing DAG navigation and synthetic stage methods."""

    requisite_stage_ref_ids: set[str]
    ref_id: str
    id: str
    parent_stage_id: str | None
    status: WorkflowStatus

    @property
    def execution(self) -> Workflow:
        raise NotImplementedError

    def has_execution(self) -> bool:
        return False

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
