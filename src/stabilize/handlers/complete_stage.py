"""
CompleteStageHandler - handles stage completion.

This is a critical handler that:
1. Determines stage status from tasks and synthetic stages
2. Plans and starts after stages
3. Plans and starts on-failure stages
4. Triggers downstream stages via startNext()
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.dag.graph import StageGraphBuilder
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteStage,
    CompleteWorkflow,
    ContinueParentStage,
    StartStage,
)
from stabilize.resilience.config import HandlerConfig
from stabilize.stages.builder import get_default_factory

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue

logger = logging.getLogger(__name__)


class CompleteStageHandler(StabilizeHandler[CompleteStage]):
    """
    Handler for CompleteStage messages.

    Execution flow:
    1. Check if stage already complete
    2. Determine status from synthetic stages and tasks
    3. If success: Plan and start after stages
    4. If failure: Plan and start on-failure stages
    5. Update stage status and end time
    6. If status allows continuation: startNext()
    7. Otherwise: CancelStage + CompleteWorkflow
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config)

    @property
    def message_type(self) -> type[CompleteStage]:
        return CompleteStage

    def handle(self, message: CompleteStage) -> None:
        """Handle the CompleteStage message.

        Retries on ConcurrencyError (optimistic lock failure) using
        configurable retry settings.
        """
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"completing stage {message.stage_id}",
        )

    def _handle_with_retry(self, message: CompleteStage) -> None:
        """Inner handle logic to be retried."""

        def on_stage(stage: StageExecution) -> None:
            # Check if already complete
            if stage.status not in {WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED}:
                logger.debug(
                    "Stage %s already has status %s",
                    stage.name,
                    stage.status,
                )
                # Stage was already completed (e.g., by JumpToStageHandler marking it TERMINAL)
                # We still need to trigger workflow completion if this is a terminal stage
                if stage.status.is_halt:
                    # Terminal status - complete workflow
                    execution = stage.execution
                    with self.repository.transaction(self.queue) as txn:
                        if message.message_id:
                            txn.mark_message_processed(
                                message_id=message.message_id,
                                handler_type="CompleteStage",
                                execution_id=message.execution_id,
                            )
                        if stage.synthetic_stage_owner is None or stage.parent_stage_id is None:
                            txn.push_message(
                                CompleteWorkflow(
                                    execution_type=execution.type.value,
                                    execution_id=execution.id,
                                )
                            )
                        else:
                            # Propagate to parent
                            txn.push_message(
                                CompleteStage(
                                    execution_type=message.execution_type,
                                    execution_id=message.execution_id,
                                    stage_id=stage.parent_stage_id,
                                )
                            )
                return

            try:
                # Determine status from tasks and synthetic stages
                status = stage.determine_status()

                # Handle after stages
                if status.is_complete and not status.is_halt:
                    after_stages = stage.first_after_stages()
                    if not after_stages:
                        self._plan_after_stages(stage)
                        after_stages = stage.first_after_stages()

                    not_started = [s for s in after_stages if s.status == WorkflowStatus.NOT_STARTED]
                    if not_started:
                        # Atomic: store stage + push all after stage messages together
                        # Store stage to persist any context changes from planning
                        with self.repository.transaction(self.queue) as txn:
                            txn.store_stage(stage)
                            for s in not_started:
                                txn.push_message(
                                    StartStage(
                                        execution_type=message.execution_type,
                                        execution_id=message.execution_id,
                                        stage_id=s.id,
                                    )
                                )
                        return

                    # If status is NOT_STARTED with no after stages, it's weird
                    if status == WorkflowStatus.NOT_STARTED:
                        logger.warning("Stage %s had no tasks or synthetic stages", stage.name)
                        status = WorkflowStatus.SKIPPED

                # Handle failure - plan on-failure stages
                elif status.is_failure:
                    has_on_failure = self._plan_on_failure_stages(stage)
                    if has_on_failure:
                        after_stages = stage.first_after_stages()
                        # Only push StartStage for on-failure stages that are NOT_STARTED
                        not_started_on_failure = [s for s in after_stages if s.status == WorkflowStatus.NOT_STARTED]
                        if not_started_on_failure:
                            # Atomic: store stage + push all on-failure stage messages together
                            # Store stage to persist any context changes from planning
                            with self.repository.transaction(self.queue) as txn:
                                txn.store_stage(stage)
                                for s in not_started_on_failure:
                                    txn.push_message(
                                        StartStage(
                                            execution_type=message.execution_type,
                                            execution_id=message.execution_id,
                                            stage_id=s.id,
                                        )
                                    )
                            return

                # Update stage status
                self.set_stage_status(stage, status)
                stage.end_time = self.current_time_millis()

                logger.info("Stage %s completed with status %s", stage.name, status)

                # Handle FAILED_CONTINUE propagation to parent
                if (
                    status == WorkflowStatus.FAILED_CONTINUE
                    and stage.synthetic_stage_owner is not None
                    and not stage.allow_sibling_stages_to_continue_on_failure
                    and stage.parent_stage_id is not None
                ):
                    # Atomic: store stage + propagate failure to parent
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
                        txn.push_message(
                            CompleteStage(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=stage.parent_stage_id,
                            )
                        )
                elif status in {
                    WorkflowStatus.SUCCEEDED,
                    WorkflowStatus.FAILED_CONTINUE,
                    WorkflowStatus.SKIPPED,
                }:
                    # Get downstream stages and parent info BEFORE transaction
                    execution = stage.execution
                    downstream_stages = self.repository.get_downstream_stages(execution.id, stage.ref_id)
                    phase = stage.synthetic_stage_owner

                    # Atomic: store stage + push all downstream/parent messages together
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)

                        # Message deduplication
                        if message.message_id:
                            txn.mark_message_processed(
                                message_id=message.message_id,
                                handler_type="CompleteStage",
                                execution_id=message.execution_id,
                            )

                        logger.debug(
                            "CompleteStage decision for %s: downstream=%d, phase=%s, parent=%s",
                            stage.name,
                            len(downstream_stages),
                            phase,
                            stage.parent_stage_id,
                        )
                        if downstream_stages:
                            # Start all downstream stages
                            for downstream in downstream_stages:
                                txn.push_message(
                                    StartStage(
                                        execution_type=execution.type.value,
                                        execution_id=execution.id,
                                        stage_id=downstream.id,
                                    )
                                )
                        elif phase is not None:
                            # Synthetic stage - notify parent
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
                        else:
                            # Terminal stage - complete workflow
                            txn.push_message(
                                CompleteWorkflow(
                                    execution_type=execution.type.value,
                                    execution_id=execution.id,
                                )
                            )
                else:
                    # Failure - atomic: store stage + cancel + complete
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
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
                            # Propagate to parent
                            txn.push_message(
                                CompleteStage(
                                    execution_type=message.execution_type,
                                    execution_id=message.execution_id,
                                    stage_id=stage.parent_stage_id,
                                )
                            )

            except Exception as e:
                logger.error(
                    "Error completing stage %s: %s",
                    stage.name,
                    e,
                    exc_info=True,
                )
                stage.context["exception"] = {
                    "details": {"error": str(e)},
                }
                self.set_stage_status(stage, WorkflowStatus.TERMINAL)
                stage.end_time = self.current_time_millis()

                # Atomic: store stage + cancel + complete workflow
                with self.repository.transaction(self.queue) as txn:
                    txn.store_stage(stage)
                    txn.push_message(
                        CancelStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=message.stage_id,
                        )
                    )
                    txn.push_message(
                        CompleteWorkflow(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                        )
                    )

        self.with_stage(message, on_stage)

    def _plan_after_stages(self, stage: StageExecution) -> None:
        """Plan after stages using the stage definition builder."""
        builder = get_default_factory().get(stage.type)
        graph = StageGraphBuilder.after_stages(stage)
        builder.after_stages(stage, graph)

        for s in graph.build():
            s.execution = stage.execution
            stage.execution.stages.append(s)  # Add to in-memory list for first_after_stages()
            self.repository.add_stage(s)

    def _plan_on_failure_stages(self, stage: StageExecution) -> bool:
        """
        Plan on-failure stages using the stage definition builder.

        Returns:
            True if on-failure stages were added
        """
        builder = get_default_factory().get(stage.type)
        graph = StageGraphBuilder.after_stages(stage)
        builder.on_failure_stages(stage, graph)

        new_stages = graph.build()
        if not new_stages:
            return False

        for s in new_stages:
            s.execution = stage.execution
            stage.execution.stages.append(s)  # Add to in-memory list for first_after_stages()
            self.repository.add_stage(s)

        return True
