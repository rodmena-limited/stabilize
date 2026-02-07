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
from stabilize.errors import is_transient
from stabilize.expressions import ExpressionError, evaluate_expression
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.stage import JoinType, SplitType
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteStage,
    CompleteWorkflow,
    ContinueParentStage,
    SkipStage,
    StartStage,
)
from stabilize.resilience.config import HandlerConfig
from stabilize.stages.builder import get_default_factory

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

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
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder)

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
            # Check if stage was reset (NOT_STARTED) - this means JumpToStageHandler
            # reset it for a retry loop, and this CompleteStage message is stale.
            # We must ignore it to prevent triggering downstream stages from a
            # previous iteration while a new iteration is starting.
            if stage.status == WorkflowStatus.NOT_STARTED:
                logger.debug(
                    "Ignoring CompleteStage for %s - stage was reset to NOT_STARTED (stale message)",
                    stage.name,
                )
                # Mark message as processed to prevent infinite reprocessing
                if message.message_id:
                    with self.repository.transaction(self.queue) as txn:
                        txn.mark_message_processed(
                            message_id=message.message_id,
                            handler_type="CompleteStage",
                            execution_id=message.execution_id,
                        )
                return

            # Check if already complete (SUCCEEDED, FAILED, TERMINAL, etc.)
            if stage.status not in {WorkflowStatus.RUNNING}:
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
                # Note: status may be RUNNING if after-stages exist but are NOT_STARTED
                # In that case, we still need to start the after-stages if core work is done
                should_handle_after_stages = status.is_complete and not status.is_halt
                if status == WorkflowStatus.RUNNING:
                    # Check if core work (tasks + before-stages) is actually done
                    # and status is RUNNING only because after-stages are NOT_STARTED
                    after_stages_check = stage.first_after_stages()
                    if after_stages_check:
                        after_statuses = {s.status for s in after_stages_check}
                        if after_statuses == {WorkflowStatus.NOT_STARTED}:
                            # All after-stages are NOT_STARTED, check if core work is done
                            task_statuses = [t.status for t in stage.tasks]
                            before_statuses = [s.status for s in stage.before_stages()]
                            all_core = before_statuses + task_statuses
                            if all_core and all(
                                s
                                in {
                                    WorkflowStatus.SUCCEEDED,
                                    WorkflowStatus.SKIPPED,
                                    WorkflowStatus.FAILED_CONTINUE,
                                }
                                for s in all_core
                            ):
                                should_handle_after_stages = True

                if should_handle_after_stages:
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

                # Record event if event recorder is configured
                if self.event_recorder:
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
                    # BLOCKING FAILURE CHECK: If stage has _blocking_failure=True and
                    # status is FAILED_CONTINUE, treat it as a hard failure that blocks
                    # downstream execution. This prevents false-positive workflow success.
                    if status == WorkflowStatus.FAILED_CONTINUE and stage.context.get("_blocking_failure", False):
                        logger.warning(
                            "Stage %s has _blocking_failure=True, converting FAILED_CONTINUE to TERMINAL",
                            stage.name,
                        )
                        status = WorkflowStatus.TERMINAL
                        self.set_stage_status(stage, status)
                        # Fall through to failure handling below
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
                                txn.push_message(
                                    CompleteStage(
                                        execution_type=message.execution_type,
                                        execution_id=message.execution_id,
                                        stage_id=stage.parent_stage_id,
                                    )
                                )
                        return
                    # Get downstream stages and parent info BEFORE transaction
                    execution = stage.execution
                    downstream_stages = self.repository.get_downstream_stages(execution.id, stage.ref_id)
                    phase = stage.synthetic_stage_owner

                    # Apply split logic to determine which downstreams to activate
                    activated_downstreams, skipped_downstreams = self._apply_split_logic(stage, downstream_stages)

                    # Track activated branches for OR-join (WCP-7)
                    if stage.split_type == SplitType.OR and activated_downstreams:
                        self._record_activated_branches(stage, activated_downstreams)

                    # Handle discriminator/N-of-M context updates on upstream completion
                    self._update_join_tracking(stage, downstream_stages)

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
                            "CompleteStage decision for %s: downstream=%d (activated=%d, skipped=%d), phase=%s, parent=%s",
                            stage.name,
                            len(downstream_stages),
                            len(activated_downstreams),
                            len(skipped_downstreams),
                            phase,
                            stage.parent_stage_id,
                        )
                        if activated_downstreams:
                            # Start activated downstream stages
                            for downstream in activated_downstreams:
                                txn.push_message(
                                    StartStage(
                                        execution_type=execution.type.value,
                                        execution_id=execution.id,
                                        stage_id=downstream.id,
                                    )
                                )
                            # Skip non-activated downstream stages (OR-split)
                            for downstream in skipped_downstreams:
                                txn.push_message(
                                    SkipStage(
                                        execution_type=execution.type.value,
                                        execution_id=execution.id,
                                        stage_id=downstream.id,
                                    )
                                )
                        elif downstream_stages and not skipped_downstreams:
                            # All downstreams came from split logic but none activated
                            # (shouldn't normally happen for AND-split)
                            for downstream in downstream_stages:
                                txn.push_message(
                                    StartStage(
                                        execution_type=execution.type.value,
                                        execution_id=execution.id,
                                        stage_id=downstream.id,
                                    )
                                )
                        elif not downstream_stages and phase is not None:
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
                        elif not downstream_stages:
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
                if is_transient(e):
                    # Transient error - re-raise to allow retry by QueueProcessor
                    # or retry_on_concurrency_error wrapper
                    raise

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
        """
        for downstream in downstream_stages:
            if downstream.join_type == JoinType.DISCRIMINATOR:
                # Track completed branch for discriminator
                completed = downstream.context.get("_completed_branches", [])
                if stage.ref_id not in completed:
                    completed.append(stage.ref_id)
                    downstream.context["_completed_branches"] = completed
                    self.repository.store_stage(downstream)

            elif downstream.join_type == JoinType.N_OF_M:
                # Track completed branch count for N-of-M join
                completed = downstream.context.get("_completed_branches", [])
                if stage.ref_id not in completed:
                    completed.append(stage.ref_id)
                    downstream.context["_completed_branches"] = completed
                    self.repository.store_stage(downstream)

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
