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

from stabilize.dag.graph import StageGraphBuilder
from stabilize.dag.readiness import PredicatePhase, evaluate_readiness
from stabilize.errors import ConcurrencyError, is_transient
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.stage import JoinType, SyntheticStageOwner
from stabilize.models.status import ACTIVE_STATUSES, WorkflowStatus
from stabilize.queue.messages import (
    CancelStage,
    CompleteStage,
    CompleteWorkflow,
    SkipStage,
    StartStage,
    StartTask,
)
from stabilize.resilience.config import HandlerConfig
from stabilize.stages.builder import get_default_factory

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)


class StartStageHandler(StabilizeHandler[StartStage]):
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
        stage.start_time = self.current_time_millis()
        self.set_stage_status(stage, WorkflowStatus.RUNNING)

        try:
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
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

    def _collect_start_messages(
        self,
        stage: StageExecution,
        message: StartStage,
    ) -> list[Message]:
        """Collect messages needed to start the stage.

        This method queries the repository but doesn't push any messages.
        The caller is responsible for pushing the returned messages atomically.
        """
        messages: list[Message] = []

        # Fetch synthetic stages (returns empty list if none)
        synthetic_stages = self.repository.get_synthetic_stages(stage.execution.id, stage.id)
        if synthetic_stages is None:
            synthetic_stages = []

        # Check for before stages
        before_stages = [
            s
            for s in synthetic_stages
            if s is not None and s.synthetic_stage_owner == SyntheticStageOwner.STAGE_BEFORE and s.is_initial()
        ]

        if before_stages:
            for before in before_stages:
                messages.append(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=before.id,
                    )
                )
            return messages

        # No before stages - start first task
        first_task = stage.first_task()
        if first_task:
            messages.append(
                StartTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=first_task.id,
                )
            )
            return messages

        # No tasks - check for after stages
        after_stages = [
            s
            for s in synthetic_stages
            if s is not None and s.synthetic_stage_owner == SyntheticStageOwner.STAGE_AFTER and s.is_initial()
        ]

        if after_stages:
            for after in after_stages:
                messages.append(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=after.id,
                    )
                )
            return messages

        # No tasks or synthetic stages - complete immediately
        messages.append(
            CompleteStage(
                execution_type=message.execution_type,
                execution_id=message.execution_id,
                stage_id=message.stage_id,
            )
        )
        return messages

    def _plan_stage(self, stage: StageExecution) -> None:
        """
        Plan the stage - build tasks and before stages.
        """
        # Hydrate context with ancestor outputs
        # This ensures tasks have access to upstream data even with partial loading
        ancestor_outputs = self.repository.get_merged_ancestor_outputs(stage.execution.id, stage.ref_id)

        merged = ancestor_outputs
        for key, value in stage.context.items():
            if key in merged and isinstance(merged[key], list) and isinstance(value, list):
                # Concatenate lists, avoiding duplicates
                existing = merged[key]
                for item in value:
                    if item not in existing:
                        existing.append(item)
            else:
                merged[key] = value

        stage.context = merged

        # Get builder
        builder = get_default_factory().get(stage.type)

        # Build tasks if none exist
        if not stage.tasks:
            stage.tasks = builder.build_tasks(stage)

        # Set task-stage back-references and mark first/last tasks
        if stage.tasks:
            for task in stage.tasks:
                task._stage = stage
            stage.tasks[0].stage_start = True
            stage.tasks[-1].stage_end = True

        # Build before stages
        graph = StageGraphBuilder.before_stages(stage)
        builder.before_stages(stage, graph)

        # Save any new synthetic stages
        for s in graph.build():
            # If not already in repository, add it
            # (StageGraphBuilder adds to execution.stages, but we need to persist)
            # Actually StageGraphBuilder usually just modifies the object graph.
            # We need to explicitly store new stages.
            # Assuming graph.build() returns new stages.
            s.execution = stage.execution  # Ensure backref
            self.repository.add_stage(s)

        # Add context flags
        builder.add_context_flags(stage)

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

    def _cancel_deferred_choice_siblings(
        self,
        stage: StageExecution,
        message: StartStage,
    ) -> None:
        """Cancel sibling stages in the same deferred choice group (WCP-16).

        When one branch of a deferred choice is claimed (set to RUNNING),
        all other branches in the same group are cancelled.
        """
        all_stages = self.repository.retrieve(stage.execution.id).stages
        for s in all_stages:
            if s.id == stage.id:
                continue
            if s.deferred_choice_group == stage.deferred_choice_group and s.status == WorkflowStatus.NOT_STARTED:
                logger.info(
                    "Deferred choice: cancelling sibling %s (group=%s) because %s was claimed",
                    s.name,
                    stage.deferred_choice_group,
                    stage.name,
                )
                self.queue.push(
                    CancelStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=s.id,
                    )
                )
