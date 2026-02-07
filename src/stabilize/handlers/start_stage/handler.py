"""
StartStageHandler - handles stage startup.

This is one of the most critical handlers in the execution engine.
It checks if upstream stages are complete, plans the stage's tasks
and synthetic stages, and starts execution.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.dag.readiness import PredicatePhase, evaluate_readiness
from stabilize.errors import ConcurrencyError, is_transient
from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.start_stage.conditions import StartStageConditionsMixin
from stabilize.handlers.start_stage.orchestration import StartStageOrchestrationMixin
from stabilize.handlers.start_stage.planner import StartStagePlannerMixin
from stabilize.models.stage import JoinType
from stabilize.models.status import ACTIVE_STATUSES, WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteStage,
    CompleteWorkflow,
    SkipStage,
    StartStage,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


class StartStageHandler(
    StartStageConditionsMixin,
    StartStageOrchestrationMixin,
    StartStagePlannerMixin,
    StabilizeHandler[StartStage],
):
    """
    Handler for StartStage messages.

    Execution flow:
    1. Check if any upstream stages failed -> CompleteWorkflow
    2. Check if all upstream stages complete
       - If not: Re-queue with retry delay
       - If yes: Continue to step 3
    3. Check if stage should be skipped -> SkipStage
    4. Check if start time expired -> SkipStage
    5. Plan the stage (build tasks and before stages)
    6. Start the stage:
       - If has before stages: StartStage for each
       - Else if has tasks: StartTask for first task
       - Else: CompleteStage
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
    def message_type(self) -> type[StartStage]:
        return StartStage

    def handle(self, message: StartStage) -> None:
        """Handle the StartStage message."""

        def on_stage(stage: StageExecution) -> None:
            try:
                # Get upstream stages from repository (returns empty list if none)
                upstream_stages = self.repository.get_upstream_stages(stage.execution.id, stage.ref_id)
                if upstream_stages is None:
                    upstream_stages = []

                # Check for jump bypass flag
                jump_bypass = bool(stage.context.get("_jump_bypass"))
                if jump_bypass:
                    # Clear the bypass flag so it doesn't persist
                    del stage.context["_jump_bypass"]

                # Evaluate readiness using pure function
                readiness = evaluate_readiness(stage, upstream_stages, jump_bypass=jump_bypass)

                # Handle readiness result based on phase
                if readiness.phase == PredicatePhase.READY:
                    logger.debug(
                        "Stage %s (%s) is ready: %s",
                        stage.name,
                        stage.id,
                        readiness.reason,
                    )
                    self._start_if_ready(stage, message)
                    return

                if readiness.phase == PredicatePhase.SKIP:
                    logger.warning(
                        "Upstream stage failed for %s (%s): %s",
                        stage.name,
                        stage.id,
                        readiness.reason,
                    )
                    self.queue.push(
                        CompleteWorkflow(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                        )
                    )
                    return

                # NOT_READY or UNDEFINED - need to wait or retry
                # Check if any upstream stage is active (RUNNING, NOT_STARTED, etc.)
                # If so, we can safely stop polling because the upstream stage
                # will trigger a new StartStage message when it completes.
                if readiness.active_upstream_ids:
                    any_active = False
                    for upstream in upstream_stages:
                        if upstream and upstream.status in ACTIVE_STATUSES:
                            any_active = True
                            logger.debug(
                                "Stage %s (%s) waiting for active upstream stage %s (%s)",
                                stage.name,
                                stage.id,
                                upstream.name,
                                upstream.status,
                            )
                            break

                    if any_active:
                        # Stop polling - wait for upstream trigger
                        return

                # Upstream not complete and not active (stuck?) - check retry count before re-queuing
                retry_count = getattr(message, "retry_count", 0) or 0
                max_retries = self.handler_config.max_stage_wait_retries

                if retry_count >= max_retries:
                    logger.error(
                        "StartStage for %s (%s) exceeded max retries (%d). "
                        "Upstream stages may be stuck. Marking as TERMINAL.",
                        stage.name,
                        stage.id,
                        max_retries,
                    )
                    self.set_stage_status(stage, WorkflowStatus.TERMINAL)
                    stage.end_time = self.current_time_millis()
                    stage.context["exception"] = {
                        "details": {"error": "Exceeded max retries waiting for upstream stages"},
                    }
                    # Use atomic transaction and propagate failure downstream
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
                        txn.push_message(
                            CompleteStage(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=stage.id,
                            )
                        )
                    return

                logger.debug(
                    "Re-queuing %s (%s) (retry %d/%d) - %s",
                    stage.name,
                    stage.id,
                    retry_count + 1,
                    max_retries,
                    readiness.reason,
                )
                # Create new message with incremented retry count
                new_message = StartStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    retry_count=retry_count + 1,
                )
                self.queue.push(new_message, self.retry_delay)

            except Exception as e:
                if is_transient(e):
                    # Transient error - re-raise to allow retry by QueueProcessor
                    raise

                logger.error(
                    "Error starting stage %s (%s): %s",
                    stage.name,
                    stage.id,
                    e,
                    exc_info=True,
                )
                error_str = str(e)

                def do_mark_error() -> None:
                    # Re-fetch stage to get current version on each retry attempt
                    fresh_stage = self.repository.retrieve_stage(message.stage_id)
                    if fresh_stage is None:
                        logger.error("Stage %s not found during error handling", message.stage_id)
                        return

                    fresh_stage.context["exception"] = {
                        "details": {"error": error_str},
                    }
                    fresh_stage.context["beforeStagePlanningFailed"] = True

                    # Atomic: store stage + push CompleteStage together
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(fresh_stage)
                        txn.push_message(
                            CompleteStage(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                            )
                        )

                self.retry_on_concurrency_error(do_mark_error, f"marking stage {stage.id} error")

        self.with_stage(message, on_stage)

    def _start_if_ready(
        self,
        stage: StageExecution,
        message: StartStage,
    ) -> None:
        """Start the stage if it's ready to run."""
        # Check if already processed
        if stage.status != WorkflowStatus.NOT_STARTED:
            # ZOMBIE DETECTION: If stage is RUNNING but has no tasks and no synthetic stages,
            # it means planning crashed before persisting tasks. We must resume planning.
            if stage.status == WorkflowStatus.RUNNING:
                has_tasks = len(stage.tasks) > 0
                synthetic_stages = self.repository.get_synthetic_stages(stage.execution.id, stage.id)
                has_synthetic = synthetic_stages is not None and len(synthetic_stages) > 0

                if not has_tasks and not has_synthetic:
                    logger.warning(
                        "Detected Zombie Stage %s (%s): RUNNING but no tasks/synthetic stages. Resuming planning.",
                        stage.name,
                        stage.id,
                    )
                    # Proceed to planning (fall through)
                    pass
                else:
                    logger.debug(
                        "Ignoring StartStage for %s - already %s",
                        stage.name,
                        stage.status,
                    )
                    return
            else:
                logger.debug(
                    "Ignoring StartStage for %s - already %s",
                    stage.name,
                    stage.status,
                )
                return

        # Check if should skip - use transaction for atomicity
        if self._should_skip(stage):
            logger.info("Skipping optional stage %s", stage.name)
            with self.repository.transaction(self.queue) as txn:
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="StartStage",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    SkipStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                    )
                )
            return

        # WCP-18: Milestone check - stage only enabled when milestone is in required status
        if self._is_milestone_expired(stage):
            logger.info("Milestone expired for stage %s, skipping", stage.name)
            with self.repository.transaction(self.queue) as txn:
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="StartStage",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    SkipStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                    )
                )
            return

        # WCP-17,39,40: Mutex check - mutual exclusion / critical section
        if self._is_mutex_blocked(stage):
            logger.debug(
                "Stage %s blocked by mutex '%s', re-queuing",
                stage.name,
                stage.mutex_key,
            )
            retry_count = getattr(message, "retry_count", 0) or 0
            new_message = StartStage(
                execution_type=message.execution_type,
                execution_id=message.execution_id,
                stage_id=message.stage_id,
                retry_count=retry_count + 1,
            )
            self.queue.push(new_message, self.retry_delay)
            return

        # WCP-16: Deferred choice - check if a sibling already claimed this group
        # Query the database directly because retrieve_stage() only loads
        # upstreams, not siblings in the same deferred_choice_group.
        if stage.deferred_choice_group and self._is_deferred_choice_claimed(stage):
            logger.info(
                "Deferred choice: sibling in group '%s' already claimed, cancelling %s",
                stage.deferred_choice_group,
                stage.name,
            )
            with self.repository.transaction(self.queue) as txn:
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="StartStage",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    CancelStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                    )
                )
            return

        # Check if start time expired - use transaction for atomicity
        if self._is_after_start_time_expiry(stage):
            logger.warning("Stage %s start time expired, skipping", stage.name)
            with self.repository.transaction(self.queue) as txn:
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="StartStage",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    SkipStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                    )
                )
            return

        # CRITICAL FIX: Claim the stage atomically BEFORE doing expensive planning.
        # This prevents race conditions where multiple handlers both pass the
        # in-memory status check and then both call _plan_stage() (which emits
        # callbacks like "PHASE START") before optimistic locking kicks in.
        # Use expected_phase="NOT_STARTED" for CAS (compare-and-swap) semantics.
        stage.start_time = self.current_time_millis()
        self.set_stage_status(stage, WorkflowStatus.RUNNING)

        try:
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage, expected_phase="NOT_STARTED")
        except ConcurrencyError:
            # Another handler already claimed this stage (race condition with
            # multiple upstream stages completing simultaneously). This is safe
            # to ignore - the stage is already being processed.
            logger.debug(
                "Ignoring duplicate StartStage for %s (concurrent claim)",
                stage.name,
            )
            return

        # WCP-16: Deferred choice - cancel sibling stages in the same group
        if stage.deferred_choice_group:
            self._cancel_deferred_choice_siblings(stage, message)

        # WCP-9/28/29: Discriminator - mark as fired after claiming
        if stage.join_type == JoinType.DISCRIMINATOR:
            stage.context["_join_fired"] = True

        # WCP-30: N-of-M - mark as fired after claiming
        if stage.join_type == JoinType.N_OF_M:
            stage.context["_join_fired"] = True

        # Now we have exclusive ownership - safe to do expensive planning
        try:
            self._plan_stage(stage)
        except Exception as e:
            logger.error(
                "Failed to plan stage %s (%s) in execution %s: %s",
                stage.name,
                stage.id,
                message.execution_id,
                e,
            )
            raise

        # Collect messages to push BEFORE starting the transaction
        messages_to_push = self._collect_start_messages(stage, message)

        # Atomic: store planned stage + push all start messages together
        try:
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)

                # Message deduplication
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="StartStage",
                        execution_id=message.execution_id,
                    )

                for msg in messages_to_push:
                    txn.push_message(msg)
        except ConcurrencyError:
            # This shouldn't happen since we already claimed the stage,
            # but handle it gracefully just in case.
            logger.warning(
                "Unexpected ConcurrencyError after claiming stage %s",
                stage.name,
            )
            return

        logger.info("Started stage %s (%s)", stage.name, stage.id)

        # Record event if event recorder is configured
        if self.event_recorder:
            self.set_event_context(stage.execution.id if stage.execution else "")
            self.event_recorder.record_stage_started(
                stage,
                source_handler="StartStageHandler",
            )
