"""The SUCCEEDED / FAILED_CONTINUE / SKIPPED completion: store, mark, and fan out in one transaction."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from stabilize.dag.readiness import PRUNED_FROM
from stabilize.queue.messages import CancelStage, CompleteStage, CompleteWorkflow, ContinueParentStage, StartStage

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.status import WorkflowStatus

logger = logging.getLogger(__name__)


def _any(handler: object) -> Any:
    return handler


class CompleteStageFanOutMixin:
    """Completes a stage and starts what follows it, retrying the whole transaction on a version conflict."""

    def _complete_and_fan_out(self, stage: StageExecution, status: WorkflowStatus, message: CompleteStage) -> None:
        self_: Any = self
        self_.retry_on_concurrency_error(
            lambda: self._fan_out_once(stage, status, message),
            f"completing stage {stage.id}",
        )

    def _fan_out_once(self, stage: StageExecution, status: WorkflowStatus, message: CompleteStage) -> None:
        handler = _any(self)
        with handler.repository.transaction(handler.queue) as txn:
            self._fan_out_in(txn, stage, status, message)

    def _halt_in(self, txn: Any, stage: StageExecution, status: WorkflowStatus, message: CompleteStage) -> None:
        """A halted stage: store it, mark the delivery, cancel its tasks and complete the workflow or the parent."""
        handler = _any(self)
        txn.store_stage(stage)
        handler._record_completion_event(stage, status)
        handler.mark_in(txn, message, "CompleteStage")
        txn.push_message(
            CancelStage(
                execution_type=message.execution_type,
                execution_id=message.execution_id,
                stage_id=message.stage_id,
            )
        )
        if stage.synthetic_stage_owner is None or stage.parent_stage_id is None:
            txn.push_message(
                CompleteWorkflow(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                )
            )
        else:
            txn.push_message(
                CompleteStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=stage.parent_stage_id,
                )
            )

    def _fan_out_in(self, txn: Any, stage: StageExecution, status: WorkflowStatus, message: CompleteStage) -> None:
        """Store the completed stage, mark the delivery and start what follows, inside *txn*."""
        handler = _any(self)
        execution = stage.execution
        downstream_stages = handler.repository.get_downstream_stages(execution.id, stage.ref_id)
        phase = stage.synthetic_stage_owner

        activated_downstreams, skipped_downstreams = handler._apply_split_logic(stage, downstream_stages)

        if True:
            txn.store_stage(stage)
            handler._record_completion_event(stage, status)
            handler._update_join_tracking(stage, downstream_stages, txn)

            if message.message_id:
                txn.mark_message_processed(
                    message_id=message.message_id,
                    handler_type="CompleteStage",
                    execution_id=message.execution_id,
                )

            if handler._rearm_multi_merge(stage, execution, txn):
                return

            logger.debug(
                "CompleteStage decision for %s: down=%d (act=%d, skip=%d), phase=%s, parent=%s",
                stage.name,
                len(downstream_stages),
                len(activated_downstreams),
                len(skipped_downstreams),
                phase,
                stage.parent_stage_id,
            )
            if activated_downstreams:
                for downstream in activated_downstreams:
                    txn.push_message(
                        StartStage(
                            execution_type=execution.type.value,
                            execution_id=execution.id,
                            stage_id=downstream.id,
                        )
                    )
                for downstream in skipped_downstreams:
                    dead_edges = set(downstream.context.get(PRUNED_FROM) or ())
                    dead_edges.add(stage.ref_id)
                    downstream.context[PRUNED_FROM] = sorted(dead_edges)
                    txn.store_stage(downstream)
                    txn.push_message(
                        StartStage(
                            execution_type=execution.type.value,
                            execution_id=execution.id,
                            stage_id=downstream.id,
                        )
                    )
            elif downstream_stages and not skipped_downstreams:
                for downstream in downstream_stages:
                    txn.push_message(
                        StartStage(
                            execution_type=execution.type.value,
                            execution_id=execution.id,
                            stage_id=downstream.id,
                        )
                    )
            elif not downstream_stages and phase is not None:
                parent_id = stage.parent_stage_id
                logger.info(
                    "Pushing ContinueParentStage for parent %s from child %s",
                    parent_id,
                    stage.name,
                )
                if parent_id:
                    txn.push_message(
                        ContinueParentStage(
                            execution_type=execution.type.value,
                            execution_id=execution.id,
                            stage_id=parent_id,
                            phase=phase,
                        )
                    )
            elif not downstream_stages:
                txn.push_message(
                    CompleteWorkflow(
                        execution_type=execution.type.value,
                        execution_id=execution.id,
                    )
                )
