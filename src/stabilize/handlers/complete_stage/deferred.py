"""Completing a member of a deferred choice that settles on completion."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from stabilize.dag.readiness import PRUNED
from stabilize.handlers.deferred_choice import settle
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import SkipStage

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.queue.messages import CompleteStage

logger = logging.getLogger("stabilize.handlers.complete_stage.handler")

DEFERRED_CHOICE_FAILED = "_deferred_choice_failed"


class CompleteStageDeferredMixin:
    """Settles the group under its lock and completes the member as winner, pruned loser or the last failure."""

    def _complete_deferred_member(
        self, stage: StageExecution, status: WorkflowStatus, message: CompleteStage, settled: dict[str, Any]
    ) -> None:
        handler: Any = self

        def attempt() -> None:
            execution = handler.repository.retrieve(message.execution_id)
            fresh = next((s for s in execution.stages if s.id == stage.id), None)
            if fresh is None or fresh.status != WorkflowStatus.RUNNING:
                with handler.repository.transaction(handler.queue) as txn:
                    handler.mark_in(txn, message, "CompleteStage")
                return
            fresh.execution = execution
            with handler.repository.transaction(handler.queue) as txn:
                decision = settle(txn, fresh, status, execution)
                now = handler.current_time_millis()
                if decision.outcome == "winner":
                    handler.set_stage_status(fresh, status)
                    fresh.end_time = now
                    for sibling in decision.live_siblings:
                        txn.push_message(
                            SkipStage(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=sibling.id,
                                pruned=True,
                            )
                        )
                    handler._fan_out_in(txn, fresh, status, message)
                elif decision.outcome in ("loser", "failed_loser"):
                    fresh.context[PRUNED] = True
                    if decision.outcome == "failed_loser":
                        fresh.context[DEFERRED_CHOICE_FAILED] = True
                    handler.set_stage_status(fresh, WorkflowStatus.SKIPPED)
                    fresh.end_time = now
                    handler._fan_out_in(txn, fresh, WorkflowStatus.SKIPPED, message)
                else:
                    handler.set_stage_status(fresh, WorkflowStatus.TERMINAL)
                    fresh.end_time = now
                    handler._halt_in(txn, fresh, WorkflowStatus.TERMINAL, message)
            logger.info(
                "Deferred choice %s: member %s completed with %s as %s",
                fresh.deferred_choice_group,
                fresh.name,
                fresh.status.name,
                decision.outcome,
            )
            settled["stage"] = fresh

        handler.retry_on_concurrency_error(attempt, f"settling deferred choice for stage {stage.id}")
