"""Helpers CompleteStageHandler uses around a stage's completion."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.dag.readiness import MM_FIRINGS, MM_TRIGGER, multi_merge_candidates
from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.complete_stage.planner import CompleteStagePlannerMixin
from stabilize.handlers.complete_stage.split_logic import CompleteStagesSplitMixin
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CompleteStage, StartStage

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.tasks.registry import TaskRegistry

logger = logging.getLogger("stabilize.handlers.complete_stage.handler")


class CompleteStageSupport(
    CompleteStagesSplitMixin,
    CompleteStagePlannerMixin,
    StabilizeHandler[CompleteStage],
):
    """Cleanup, completion events and multi-merge re-arming for CompleteStageHandler."""

    task_registry: TaskRegistry | None

    def _invoke_task_cleanup(self, stage: StageExecution) -> None:
        """Invoke on_cleanup() on all task implementations in the stage.

        Called when a stage reaches a terminal state (SUCCEEDED, FAILED,
        TERMINAL, CANCELED, SKIPPED, etc.) to allow tasks to release
        resources such as containers, connections, or temporary files.

        Errors in cleanup are logged but never propagate — cleanup must
        not prevent stage/workflow completion.
        """
        if self.task_registry is None:
            return

        for task_model in stage.tasks:
            try:
                task_impl = self.task_registry.get_by_class(task_model.implementing_class)
                task_impl.on_cleanup(stage)
            except Exception as e:
                logger.warning(
                    "Error in on_cleanup for task %s (type=%s) in stage %s: %s",
                    task_model.name,
                    task_model.implementing_class,
                    stage.name,
                    e,
                )

    def _record_completion_event(self, stage: StageExecution, status: WorkflowStatus) -> None:
        """Record the stage completion/failure/skip event.

        Must be called INSIDE the branch's store transaction so the event
        joins the same commit as the state change (no phantom events) and
        its bus publication is deferred until after commit.
        """
        if not self.event_recorder:
            return
        self.set_event_context(stage.execution.id if stage.execution else "")
        if status.is_failure:
            error = stage.context.get("exception", {}).get("details", {}).get("error", "Unknown error")
            self.event_recorder.record_stage_failed(
                stage,
                error=str(error),
                source_handler="CompleteStageHandler",
            )
        elif status == WorkflowStatus.SKIPPED:
            self.event_recorder.record_stage_skipped(
                stage,
                reason="Skipped",
                source_handler="CompleteStageHandler",
            )
        else:
            self.event_recorder.record_stage_completed(
                stage,
                source_handler="CompleteStageHandler",
            )

    def _rearm_multi_merge(self, stage, execution, txn) -> bool:  # type: ignore[no-untyped-def]
        """Re-arm a multi-merge stage for its next upstream. True if re-armed.

        Archives the firing that just completed, then resets the row in place.
        The status assignment is direct because VALID_TRANSITIONS has no edge out
        of SUCCEEDED; reset_stage_for_retry sets the same precedent.
        """
        from stabilize.models.stage import JoinType

        if stage.join_type != JoinType.MULTI_MERGE:
            return False

        upstreams = self.repository.get_upstream_stages(execution.id, stage.ref_id) or []
        candidates = multi_merge_candidates(stage, upstreams)
        if not candidates:
            return False

        firings = list(stage.context.get(MM_FIRINGS) or ())
        firings.append(
            {
                "trigger": stage.context.get(MM_TRIGGER),
                "status": stage.status.name,
                "outputs": dict(stage.outputs or {}),
            }
        )
        stage.context[MM_FIRINGS] = firings

        stage.status = WorkflowStatus.NOT_STARTED
        stage.start_time = None
        stage.end_time = None
        stage.outputs = {}
        for task in stage.tasks:
            task.status = WorkflowStatus.NOT_STARTED
            task.start_time = None
            task.end_time = None

        txn.store_stage(stage)
        txn.push_message(
            StartStage(
                execution_type=execution.type.value,
                execution_id=execution.id,
                stage_id=stage.id,
                triggering_upstream_ref_id=candidates[0],
            )
        )
        logger.info(
            "Multi-merge %s re-armed for upstream %s (%d firing(s) so far)",
            stage.name,
            candidates[0],
            len(firings),
        )
        return True
