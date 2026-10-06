"""StartStageHandler's start step: claim, plan and start a stage that is ready."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.dag.readiness import MM_CONSUMED, MM_TRIGGER, multi_merge_candidates
from stabilize.errors import ConcurrencyError
from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.start_stage.conditions import StartStageConditionsMixin
from stabilize.handlers.start_stage.orchestration import StartStageOrchestrationMixin
from stabilize.handlers.start_stage.planner import StartStagePlannerMixin
from stabilize.models.stage import JoinType
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CancelStage, SkipStage, StartStage

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger("stabilize.handlers.start_stage.handler")


class _ClaimBlockedError(Exception):
    """Raised inside the claim transaction when a mutex or deferred-choice
    claim is held by another live stage; rolls the transaction back."""

    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


class StartStageStarting(
    StartStageConditionsMixin,
    StartStageOrchestrationMixin,
    StartStagePlannerMixin,
    StabilizeHandler[StartStage],
):
    """The start step of StartStageHandler."""

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
        #
        # A zombie re-plan (stage already RUNNING with no tasks/synthetics —
        # the original claimer crashed between claim and plan) must CAS
        # against the row's actual RUNNING phase: expecting NOT_STARTED can
        # never succeed there, so every recovery attempt would be swallowed
        # as a "duplicate claim" and the workflow wedged forever. The version
        # check in store_stage still serializes concurrent re-planners.
        if stage.status == WorkflowStatus.RUNNING:
            claim_expected_phase = "RUNNING"
        else:
            claim_expected_phase = "NOT_STARTED"
            stage.start_time = self.current_time_millis()
            self.set_stage_status(stage, WorkflowStatus.RUNNING)

        try:
            with self.repository.transaction(self.queue) as txn:
                # WCP-17/39/40 + WCP-16: the read-then-check fast paths above
                # (_is_mutex_blocked / _is_deferred_choice_claimed) cannot
                # serialize two DIFFERENT sibling stages racing past them —
                # each sibling's per-row CAS succeeds on its own row. Mutual
                # exclusion is enforced here, inside the claim transaction,
                # via a unique (execution_id, claim_key) row.
                if stage.mutex_key and not txn.acquire_claim(
                    message.execution_id,
                    f"mutex:{stage.mutex_key}",
                    stage.id,
                    steal_if_owner_terminal=True,
                ):
                    raise _ClaimBlockedError("mutex")
                if stage.deferred_choice_group and not txn.acquire_claim(
                    message.execution_id,
                    f"choice:{stage.deferred_choice_group}",
                    stage.id,
                ):
                    raise _ClaimBlockedError("choice")
                txn.store_stage(stage, expected_phase=claim_expected_phase)
        except _ClaimBlockedError as blocked:
            # The claim transaction rolled back: this stage did not start.
            stage.status = WorkflowStatus.NOT_STARTED
            stage.start_time = None
            if blocked.kind == "mutex":
                # Mutex held by a live sibling - wait and retry.
                logger.debug(
                    "Stage %s lost mutex claim '%s', re-queuing",
                    stage.name,
                    stage.mutex_key,
                )
                retry_count = getattr(message, "retry_count", 0) or 0
                self.queue.push(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        retry_count=retry_count + 1,
                    ),
                    self.retry_delay,
                )
            else:
                # Deferred choice already decided - cancel this branch.
                logger.info(
                    "Stage %s lost deferred choice claim '%s', cancelling",
                    stage.name,
                    stage.deferred_choice_group,
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

        # WCP-8: Multi-merge - record which upstream this firing consumes, so a
        # later completion of a different upstream fires again and the same one
        # never fires twice.
        if stage.join_type == JoinType.MULTI_MERGE:
            upstreams = self.repository.get_upstream_stages(stage.execution.id, stage.ref_id) or []
            candidates = multi_merge_candidates(stage, upstreams)
            trigger = message.triggering_upstream_ref_id
            if trigger not in candidates:
                trigger = candidates[0] if candidates else ""
            if trigger:
                consumed = list(stage.context.get(MM_CONSUMED) or ())
                consumed.append(trigger)
                stage.context[MM_CONSUMED] = consumed
                stage.context[MM_TRIGGER] = trigger

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
