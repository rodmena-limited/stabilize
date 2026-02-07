"""
Condition-checking methods for StartStageHandler.

These methods determine whether a stage should be skipped, expired,
blocked, or claimed by a deferred choice.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class StartStageConditionsMixin:
    """Mixin providing condition-check methods used by StartStageHandler."""

    if TYPE_CHECKING:
        repository: WorkflowStore
        queue: Queue

        def current_time_millis(self) -> int: ...

    def _should_skip(self, stage: StageExecution) -> bool:
        """
        Check if stage should be skipped.

        Checks the 'stageEnabled' context for conditional execution.
        """
        stage_enabled = stage.context.get("stageEnabled")
        if stage_enabled is None:
            return False

        if isinstance(stage_enabled, dict):
            expr_type = stage_enabled.get("type")
            if expr_type == "expression":
                # TODO: Evaluate expression
                expression = stage_enabled.get("expression", "true")
                # For now, just check if it's explicitly "false"
                if isinstance(expression, str):
                    return expression.lower() == "false"

        return False

    def _is_after_start_time_expiry(self, stage: StageExecution) -> bool:
        """Check if current time is past start time expiry."""
        if stage.start_time_expiry is None:
            return False
        return self.current_time_millis() > stage.start_time_expiry

    def _is_milestone_expired(self, stage: StageExecution) -> bool:
        """Check if the milestone condition for this stage has expired (WCP-18).

        A milestone stage must be in the required status. If the milestone
        stage has already progressed past the required status, the activity
        can never be enabled and should be skipped.
        """
        if not stage.milestone_ref_id or not stage.milestone_status:
            return False

        all_stages = self.repository.retrieve(stage.execution.id).stages
        milestone_stage = None
        for s in all_stages:
            if s.ref_id == stage.milestone_ref_id:
                milestone_stage = s
                break

        if milestone_stage is None:
            logger.warning(
                "Milestone stage %s not found for stage %s",
                stage.milestone_ref_id,
                stage.name,
            )
            return True  # Can't verify milestone, skip

        required_status = WorkflowStatus[stage.milestone_status]

        # If milestone is in the required status, the stage is enabled
        if milestone_stage.status == required_status:
            return False

        # If milestone has already completed (progressed past required), expired
        if milestone_stage.status.is_complete:
            return True

        # Milestone is still active but not in required status yet
        # This could mean it hasn't reached the milestone yet - don't skip
        return False

    def _is_deferred_choice_claimed(self, stage: StageExecution) -> bool:
        """Check if a sibling in the deferred choice group already started (WCP-16).

        Queries ALL stages of the execution to find siblings in the same group.
        Returns True if any sibling has progressed past NOT_STARTED.

        CONCURRENCY NOTE: This method performs a read-then-check which is inherently
        non-atomic. The caller MUST use store_stage(stage, expected_phase="NOT_STARTED")
        when transitioning the stage to RUNNING to atomically claim the deferred choice.
        If the atomic store fails (ConcurrencyError), another stage in the group has
        already claimed it.
        """
        if not stage.deferred_choice_group:
            return False

        all_stages = self.repository.retrieve(stage.execution.id).stages
        for s in all_stages:
            if s.id == stage.id:
                continue
            if s.deferred_choice_group == stage.deferred_choice_group and s.status != WorkflowStatus.NOT_STARTED:
                return True
        return False

    def _is_mutex_blocked(self, stage: StageExecution) -> bool:
        """Check if another stage with the same mutex_key is RUNNING (WCP-17,39,40).

        Queries ALL stages of the execution for mutual exclusion.

        CONCURRENCY NOTE: This method performs a read-then-check which is inherently
        non-atomic. The caller MUST use store_stage(stage, expected_phase="NOT_STARTED")
        when transitioning the stage to RUNNING to atomically acquire the mutex.
        If the atomic store fails (ConcurrencyError), another stage with the same
        mutex_key has already acquired it, and this stage should retry later.
        """
        if not stage.mutex_key:
            return False

        all_stages = self.repository.retrieve(stage.execution.id).stages
        for s in all_stages:
            if s.id == stage.id:
                continue
            if s.mutex_key == stage.mutex_key and s.status == WorkflowStatus.RUNNING:
                return True
        return False
