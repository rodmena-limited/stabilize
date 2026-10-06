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

from stabilize.dag.readiness import PRUNED, PredicatePhase, evaluate_readiness
from stabilize.errors import is_transient
from stabilize.handlers.start_stage.starting import StartStageStarting
from stabilize.models.stage.stage import PLANNING_FAILED
from stabilize.models.status import ACTIVE_STATUSES, WorkflowStatus
from stabilize.queue.messages import (
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




class StartStageHandler(StartStageStarting):
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

                if readiness.phase == PredicatePhase.PRUNE:
                    logger.info(
                        "Pruning stage %s (%s): %s",
                        stage.name,
                        stage.id,
                        readiness.reason,
                    )
                    with self.repository.transaction(self.queue) as txn:
                        stage.context[PRUNED] = True
                        txn.store_stage(stage)
                        if message.message_id:
                            txn.mark_message_processed(
                                message_id=message.message_id,
                                handler_type="StartStage",
                                execution_id=message.execution_id,
                            )
                        # SkipStage performs the SKIPPED transition, records the
                        # event, and fans out to this stage's own children, which
                        # then evaluate their own edges and prune in turn.
                        txn.push_message(
                            SkipStage(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=stage.id,
                            )
                        )
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
                    fresh_stage.context[PLANNING_FAILED] = True

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
