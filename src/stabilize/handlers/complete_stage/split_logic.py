"""Split logic mixin for CompleteStageHandler."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.expressions import ExpressionError, evaluate_expression
from stabilize.models.stage import JoinType, SplitType

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore

logger = logging.getLogger(__name__)


class CompleteStagesSplitMixin:
    """Mixin providing split/join logic for stage completion."""

    if TYPE_CHECKING:
        repository: WorkflowStore

    def _apply_split_logic(
        self,
        stage: StageExecution,
        downstream_stages: list[StageExecution],
    ) -> tuple[list[StageExecution], list[StageExecution]]:
        """Apply split logic to determine which downstreams to activate.

        For AND-split (default): activates all downstream stages.
        For OR-split (WCP-6): evaluates conditions per downstream and only
        activates matching ones.

        Returns:
            Tuple of (activated_stages, skipped_stages).
        """
        if not downstream_stages:
            return [], []

        if stage.split_type != SplitType.OR or not stage.split_conditions:
            # AND-split: activate all
            return downstream_stages, []

        # OR-split: evaluate conditions per downstream
        # Merge context and outputs for condition evaluation so that
        # conditions can reference both stage context and task outputs.
        eval_context = dict(stage.context)
        eval_context.update(stage.outputs)

        activated: list[StageExecution] = []
        skipped: list[StageExecution] = []

        for downstream in downstream_stages:
            condition = stage.split_conditions.get(downstream.ref_id)
            if condition is None:
                # No condition specified for this downstream - activate by default
                activated.append(downstream)
                continue

            try:
                result = evaluate_expression(condition, eval_context)
                if result:
                    activated.append(downstream)
                else:
                    skipped.append(downstream)
            except ExpressionError:
                logger.warning(
                    "Failed to evaluate split condition '%s' for downstream %s, skipping",
                    condition,
                    downstream.ref_id,
                )
                skipped.append(downstream)

        # OR-split must activate at least one branch
        if not activated and downstream_stages:
            logger.warning(
                "OR-split for stage %s activated no branches, activating first by default",
                stage.name,
            )
            activated.append(downstream_stages[0])
            skipped = [d for d in downstream_stages[1:]]

        return activated, skipped

    def _record_activated_branches(
        self,
        stage: StageExecution,
        activated_downstreams: list[StageExecution],
    ) -> None:
        """Record activated branch ref_ids for paired OR-join stages (WCP-7).

        Finds downstream stages that have join_type=OR and sets their
        '_activated_branches' context with the ref_ids of activated branches.
        """
        activated_ref_ids = [d.ref_id for d in activated_downstreams]

        # Look for OR-join stages downstream that need branch tracking
        execution = stage.execution
        for s in execution.stages:
            if s.join_type == JoinType.OR:
                # Check if any of this stage's activated downstreams are upstream of the OR-join
                if s.requisite_stage_ref_ids & set(activated_ref_ids):
                    # This OR-join depends on some of our activated branches
                    existing = s.context.get("_activated_branches", [])
                    merged = list(set(existing) | set(activated_ref_ids))
                    s.context["_activated_branches"] = merged
                    self.repository.store_stage(s)

    def _update_join_tracking(
        self,
        stage: StageExecution,
        downstream_stages: list[StageExecution],
    ) -> None:
        """Update join tracking context for discriminator and N-of-M joins.

        When a stage completes, check if any of its downstream stages use
        special join types that need tracking updates.

        Uses retry loop with expected_phase for atomic updates to prevent
        race conditions when multiple upstreams complete simultaneously.
        """
        from stabilize.errors import ConcurrencyError

        for downstream in downstream_stages:
            if downstream.join_type == JoinType.DISCRIMINATOR:
                # Track completed branch for discriminator with retry
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        # Re-read downstream to get latest version
                        fresh_downstream = self.repository.retrieve_stage(downstream.id)
                        completed = fresh_downstream.context.get("_completed_branches", [])
                        if stage.ref_id not in completed:
                            completed.append(stage.ref_id)
                            fresh_downstream.context["_completed_branches"] = completed
                            # Use expected_phase for atomic update
                            self.repository.store_stage(
                                fresh_downstream, expected_phase=fresh_downstream.status.name
                            )
                        break
                    except ConcurrencyError:
                        if attempt == max_retries - 1:
                            raise

            elif downstream.join_type == JoinType.N_OF_M:
                # Track completed branch count for N-of-M join with retry
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        # Re-read downstream to get latest version
                        fresh_downstream = self.repository.retrieve_stage(downstream.id)
                        completed = fresh_downstream.context.get("_completed_branches", [])
                        if stage.ref_id not in completed:
                            completed.append(stage.ref_id)
                            fresh_downstream.context["_completed_branches"] = completed
                            # Use expected_phase for atomic update
                            self.repository.store_stage(
                                fresh_downstream, expected_phase=fresh_downstream.status.name
                            )
                        break
                    except ConcurrencyError:
                        if attempt == max_retries - 1:
                            raise
