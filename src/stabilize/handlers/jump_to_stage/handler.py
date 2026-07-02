"""
JumpToStageHandler - handles dynamic routing jumps.

This handler processes JumpToStage messages to redirect workflow execution
to a different stage. It's used by TaskResult.jump_to() for dynamic flow
control patterns like retry loops, conditional branching, and error recovery.

The handler:
1. Validates the target stage exists
2. Checks jump count to prevent infinite loops (default max: 10)
3. Resets the target stage to NOT_STARTED
4. Merges jump context into target stage
5. Records jump history for debugging
6. Pushes StartStage for the target
"""

from __future__ import annotations

import logging
from datetime import timedelta
from functools import partial
from typing import TYPE_CHECKING, Any

from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.jump_to_stage.reset import (
    reset_stage_for_retry,
    reset_stage_to_skipped,
    reset_stage_to_succeeded,
    reset_stage_to_terminal,
)
from stabilize.handlers.jump_to_stage.traversal import (
    get_downstream_stages,
    get_resettable_downstream_stages,
    get_skipped_stages,
)
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.transaction import TransactionHelper
from stabilize.queue.messages import (
    CompleteStage,
    JumpToStage,
    StartStage,
)
from stabilize.resilience.config import HandlerConfig

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)

# Default maximum number of jumps allowed per execution
DEFAULT_MAX_JUMPS = 10


class JumpToStageHandler(StabilizeHandler[JumpToStage]):
    """
    Handler for JumpToStage messages.

    Execution flow:
    1. Validate target stage exists
    2. Check jump count < max (default 10)
    3. Reset target stage to NOT_STARTED
    4. Merge jump context into target stage
    5. Increment jump counter and record history
    6. Push StartStage for target
    """

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        retry_delay: timedelta | None = None,
        handler_config: HandlerConfig | None = None,
        event_recorder: EventRecorder | None = None,
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config, event_recorder=event_recorder)
        self.txn_helper = TransactionHelper(repository, queue)

    @property
    def message_type(self) -> type[JumpToStage]:
        return JumpToStage

    def handle(self, message: JumpToStage) -> None:
        """Handle the JumpToStage message."""
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"jumping to stage {message.target_stage_ref_id}",
        )

    def _synthetic_reset_mutations(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[tuple[str, Any]]:
        """Collect reset mutations for a stage's synthetic children."""
        synthetics = self.repository.get_synthetic_stages(execution_id, parent_stage_id) or []
        return [(s.id, reset_stage_for_retry) for s in synthetics]

    def _apply_jump(
        self,
        mutations: list[tuple[str, Any]],
        message: JumpToStage,
        messages_to_push: list[Any],
    ) -> None:
        """Apply every stage mutation of a jump plus its follow-on messages in
        ONE transaction.

        A jump that commits stage-by-stage can crash half-applied: the source
        already SUCCEEDED while skip-region stages are still startable, which
        recovery then starts, contrary to the jump's semantics. Each attempt
        reloads fresh rows so optimistic-lock retries operate on current
        versions; the whole transaction rolls back and retries on conflict.
        """

        def attempt() -> None:
            with self.repository.transaction(self.queue) as txn:
                for stage_id, mutate in mutations:
                    fresh = self.repository.retrieve_stage(stage_id)
                    if fresh is None:
                        logger.warning("Stage %s not found during jump; skipping", stage_id)
                        continue
                    mutate(fresh)
                    txn.store_stage(fresh)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="JumpToStage",
                        execution_id=message.execution_id,
                    )
                for msg in messages_to_push:
                    txn.push_message(msg)

        self.retry_on_concurrency_error(attempt, "applying jump atomically")

    def _handle_with_retry(self, message: JumpToStage) -> None:
        """Handle jump with concurrency retry support."""

        def on_stage(_partial_stage: StageExecution) -> None:
            # Retrieve full execution to access all stages
            # (retrieve_stage only returns partial execution with one stage)
            execution = self.repository.retrieve(message.execution_id)

            # Get source stage from full execution for consistency
            source_stage = next(
                (s for s in execution.stages if s.id == message.stage_id),
                None,
            )
            if source_stage is None:
                logger.error(
                    "Source stage not found: %s (execution: %s)",
                    message.stage_id,
                    message.execution_id,
                )
                return

            # Find target stage by ref_id
            target_stage = execution.stage_by_ref_id(message.target_stage_ref_id)

            if target_stage is None:
                self._handle_target_not_found(message, source_stage)
                return

            # Check jump count to prevent infinite loops
            if not self._check_jump_count(message, execution, source_stage):
                return

            # Reset target stage (in-memory; persisted atomically below)
            reset_stage_for_retry(target_stage)

            # Collect every stage mutation of this jump; they are applied in
            # ONE transaction together with the StartStage push at the end.
            mutations: list[tuple[str, Any]] = []

            # Reset all downstream stages that depend on target (for retry loops)
            for downstream in get_resettable_downstream_stages(execution, target_stage.ref_id):
                if downstream.id != source_stage.id and downstream.id != target_stage.id:
                    logger.debug("Resetting downstream stage: %s", downstream.ref_id)
                    mutations.append((downstream.id, reset_stage_for_retry))
                    mutations.extend(self._synthetic_reset_mutations(message.execution_id, downstream.id))

            # Determine if this is a forward or backward jump
            downstream_stages = get_downstream_stages(execution, target_stage.ref_id)
            is_self_loop = source_stage.id == target_stage.id
            is_backward_jump = is_self_loop or source_stage in downstream_stages

            logger.info(
                "Jump direction: %s (source=%s, target=%s, downstream=%s)",
                "backward" if is_backward_jump else "forward",
                source_stage.ref_id,
                target_stage.ref_id,
                [s.ref_id for s in downstream_stages],
            )

            # Handle source stage based on jump direction
            if not is_backward_jump:
                # Forward jump: mark all stages between source and target SKIPPED
                end_time = self.current_time_millis()
                for skipped in get_skipped_stages(execution, source_stage, target_stage):
                    if skipped.status == WorkflowStatus.NOT_STARTED:
                        logger.debug("Marking skipped stage: %s", skipped.ref_id)
                        mutations.append(
                            (skipped.id, partial(reset_stage_to_skipped, end_time=end_time))
                        )

            # Get jump count for context updates
            jump_count = source_stage.context.get("_jump_count", 0)
            # Use explicit None checks to allow max_jumps=0 (disables jumps)
            max_jumps = execution.context.get("_max_jumps")
            if max_jumps is None:
                max_jumps = source_stage.context.get("_max_jumps")
            if max_jumps is None:
                max_jumps = DEFAULT_MAX_JUMPS
            new_jump_count = jump_count + 1

            # Merge jump context into target stage
            if message.jump_context:
                target_stage.context.update(message.jump_context)

            # Make jump outputs available via special context key
            if message.jump_outputs:
                target_stage.context["_jump_outputs"] = message.jump_outputs

            # Set bypass flag so StartStageHandler skips prerequisite checks
            target_stage.context["_jump_bypass"] = True

            # Record jump history
            jump_history = source_stage.context.get("_jump_history", [])
            jump_history.append(
                {
                    "from_stage": source_stage.ref_id,
                    "to_stage": message.target_stage_ref_id,
                    "jump_number": new_jump_count,
                    "context_keys": (list(message.jump_context.keys()) if message.jump_context else []),
                }
            )

            # Store jump metadata in target stage
            target_stage.context["_jump_count"] = new_jump_count
            target_stage.context["_jump_history"] = jump_history

            logger.info(
                "Jumping from stage %s to %s (jump #%d/%d)",
                source_stage.ref_id,
                message.target_stage_ref_id,
                new_jump_count,
                max_jumps,
            )

            # Source stage mutation if not self-loop
            if not is_self_loop:
                source_context_updates = {
                    "_jump_count": new_jump_count,
                    "_jump_history": jump_history,
                }
                if is_backward_jump:

                    def mutate_source(s: StageExecution, updates: dict[str, Any] = source_context_updates) -> None:
                        reset_stage_for_retry(s)
                        s.context.update(updates)

                else:
                    source_end_time = self.current_time_millis()

                    def mutate_source(s: StageExecution, updates: dict[str, Any] = source_context_updates) -> None:
                        reset_stage_to_succeeded(s, source_end_time)
                        s.context.update(updates)

                mutations.append((source_stage.id, mutate_source))

                # Reset synthetic stages for source if backward jump
                if is_backward_jump:
                    mutations.extend(self._synthetic_reset_mutations(message.execution_id, source_stage.id))

            # Target stage mutation
            target_context_updates = dict(target_stage.context)

            def mutate_target(s: StageExecution, updates: dict[str, Any] = target_context_updates) -> None:
                reset_stage_for_retry(s)
                s.context.update(updates)

            mutations.append((target_stage.id, mutate_target))

            # Reset synthetic stages for target
            mutations.extend(self._synthetic_reset_mutations(message.execution_id, target_stage.id))

            # Apply every mutation + mark processed + StartStage push in ONE
            # transaction: a crash rolls the whole jump back to a consistent
            # pre-jump state instead of leaving it half-applied.
            self._apply_jump(
                mutations,
                message,
                [
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=target_stage.id,
                    )
                ],
            )

        self.with_stage(message, on_stage)

    def _handle_target_not_found(
        self,
        message: JumpToStage,
        source_stage: StageExecution,
    ) -> None:
        """Handle case when target stage is not found."""
        logger.error(
            "Jump target stage not found: %s (execution: %s)",
            message.target_stage_ref_id,
            message.execution_id,
        )
        context_updates = {"jump_error": f"Target stage not found: {message.target_stage_ref_id}"}
        end_time = self.current_time_millis()

        def mutate(s: StageExecution) -> None:
            reset_stage_to_terminal(s, end_time)
            s.context.update(context_updates)

        self._apply_jump(
            [(source_stage.id, mutate)],
            message,
            [
                CompleteStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=source_stage.id,
                )
            ],
        )

    def _check_jump_count(
        self,
        message: JumpToStage,
        execution: Workflow,
        source_stage: StageExecution,
    ) -> bool:
        """Check if jump count is within limits. Returns False if exceeded."""

        jump_count = source_stage.context.get("_jump_count", 0)
        # Use explicit None checks to allow max_jumps=0 (disables jumps)
        max_jumps = execution.context.get("_max_jumps")
        if max_jumps is None:
            max_jumps = source_stage.context.get("_max_jumps")
        if max_jumps is None:
            max_jumps = DEFAULT_MAX_JUMPS

        if jump_count >= max_jumps:
            logger.error(
                "Max jump count exceeded (%d) for execution %s",
                max_jumps,
                message.execution_id,
            )
            context_updates = {"jump_error": f"Max jump count exceeded: {jump_count}/{max_jumps}"}
            end_time = self.current_time_millis()

            def mutate(s: StageExecution) -> None:
                reset_stage_to_terminal(s, end_time)
                s.context.update(context_updates)

            self._apply_jump(
                [(source_stage.id, mutate)],
                message,
                [
                    CompleteStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=source_stage.id,
                    )
                ],
            )
            return False
        return True
