"""Split logic mixin for CompleteStageHandler."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.expressions import ExpressionError, evaluate_expression
from stabilize.models.stage import JoinType, SplitType

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.persistence.store.transaction import StoreTransaction

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

    def _update_join_tracking(
        self,
        stage: StageExecution,
        downstream_stages: list[StageExecution],
        txn: StoreTransaction,
    ) -> None:
        """Record *stage* as a completed branch on each discriminator or N-of-M downstream, inside *txn*.

        A version conflict raises ConcurrencyError out of the transaction; the caller retries the whole
        completion with fresh rows.
        """
        for downstream in downstream_stages:
            if downstream.join_type not in (JoinType.DISCRIMINATOR, JoinType.N_OF_M):
                continue
            fresh_downstream = self.repository.retrieve_stage(downstream.id)
            if fresh_downstream is None:
                continue
            completed = list(fresh_downstream.context.get("_completed_branches", []))
            if stage.ref_id in completed:
                continue
            completed.append(stage.ref_id)
            fresh_downstream.context["_completed_branches"] = completed
            txn.store_stage(fresh_downstream, expected_phase=fresh_downstream.status.name)
