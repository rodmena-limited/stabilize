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
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.handlers.jump_to_stage.reset import (
    reset_stage_for_retry,
    reset_stage_to_skipped,
    reset_stage_to_succeeded,
    reset_stage_to_terminal,
)
from stabilize.handlers.jump_to_stage.traversal import (
    get_downstream_stages,
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
    from stabilize.models.stage import StageExecution
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
    ) -> None:
        super().__init__(queue, repository, retry_delay, handler_config)
        self.txn_helper = TransactionHelper(repository, queue)

    @property
    def message_type(self) -> type[JumpToStage]:
        return JumpToStage

    def _reload_reset_and_store(
        self,
        stage_id: str,
        context_updates: dict | None = None,
        target_status: WorkflowStatus = WorkflowStatus.NOT_STARTED,
    ) -> None:
        """Reload a stage from DB, apply status change, and store it.

        This pattern ensures we always have the latest version number before
        storing, preventing optimistic locking failures due to stale versions.

        Args:
            stage_id: The stage ID to reload and update
            context_updates: Optional context updates to apply
            target_status: The status to set (NOT_STARTED, SUCCEEDED, SKIPPED)
        """
        # Reload fresh from DB to get current version
        fresh_stage = self.repository.retrieve_stage(stage_id)
        if fresh_stage is None:
            logger.warning("Stage %s not found during reload", stage_id)
            return

        end_time = self.current_time_millis()

        if target_status == WorkflowStatus.NOT_STARTED:
            reset_stage_for_retry(fresh_stage)
        elif target_status == WorkflowStatus.SUCCEEDED:
            reset_stage_to_succeeded(fresh_stage, end_time)
        elif target_status == WorkflowStatus.SKIPPED:
            reset_stage_to_skipped(fresh_stage, end_time)
        elif target_status == WorkflowStatus.TERMINAL:
            reset_stage_to_terminal(fresh_stage, end_time)
        else:
            fresh_stage.status = target_status

        if context_updates:
            fresh_stage.context.update(context_updates)

        self.repository.store_stage(fresh_stage)

    def _reset_synthetics_for_stage(self, execution_id: str, parent_stage_id: str) -> None:
        """Reset all synthetic stages for a given parent stage.

        This ensures that when a stage is reset (e.g. for retry), its synthetic
        stages (setup/teardown) are also reset and re-executed.
        """
        synthetics = self.repository.get_synthetic_stages(execution_id, parent_stage_id)
        if not synthetics:
            return

        for stage in synthetics:
            logger.debug("Resetting synthetic stage: %s", stage.ref_id)
            self.retry_on_concurrency_error(
                partial(self._reload_reset_and_store, stage.id),
                f"resetting synthetic stage {stage.ref_id}",
            )

    def handle(self, message: JumpToStage) -> None:
        """Handle the JumpToStage message."""
        self.retry_on_concurrency_error(
            lambda: self._handle_with_retry(message),
            f"jumping to stage {message.target_stage_ref_id}",
        )

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

            # Reset target stage
            reset_stage_for_retry(target_stage)

            # Reset all downstream stages that depend on target (for retry loops)
            self._reset_downstream_stages(message, execution, source_stage, target_stage)

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
            self._handle_source_stage(message, execution, source_stage, target_stage, is_backward_jump)

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

            # Store source stage if not self-loop
            if not is_self_loop:
                source_context_updates = {
                    "_jump_count": new_jump_count,
                    "_jump_history": jump_history,
                }
                source_target_status = WorkflowStatus.NOT_STARTED if is_backward_jump else WorkflowStatus.SUCCEEDED
                self.retry_on_concurrency_error(
                    partial(
                        self._reload_reset_and_store,
                        source_stage.id,
                        source_context_updates,
                        source_target_status,
                    ),
                    f"storing source stage {source_stage.ref_id}",
                )

                # Reset synthetic stages for source if backward jump
                if is_backward_jump:
                    self._reset_synthetics_for_stage(message.execution_id, source_stage.id)

            # Store target stage
            target_context_updates = dict(target_stage.context)

            def reload_and_store_target() -> None:
                fresh_target = self.repository.retrieve_stage(target_stage.id)
                if fresh_target is None:
                    logger.error("Target stage %s not found during reload", target_stage.id)
                    return
                reset_stage_for_retry(fresh_target)
                fresh_target.context.update(target_context_updates)
                self.repository.store_stage(fresh_target)

            self.retry_on_concurrency_error(
                reload_and_store_target,
                f"storing target stage {target_stage.ref_id}",
            )

            # Reset synthetic stages for target
            self._reset_synthetics_for_stage(message.execution_id, target_stage.id)

            # Push StartStage message atomically
            self.txn_helper.execute_atomic(
                stage=None,
                source_message=message,
                messages_to_push=[
                    (
                        StartStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=target_stage.id,
                        ),
                        None,
                    )
                ],
                handler_name="JumpToStage",
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
        self.retry_on_concurrency_error(
            partial(
                self._reload_reset_and_store,
                source_stage.id,
                context_updates,
                WorkflowStatus.TERMINAL,
            ),
            f"storing terminal stage {source_stage.ref_id}",
        )

        self.txn_helper.execute_atomic(
            stage=None,
            source_message=message,
            messages_to_push=[
                (
                    CompleteStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=source_stage.id,
                    ),
                    None,
                )
            ],
            handler_name="JumpToStage",
        )

    def _check_jump_count(
        self,
        message: JumpToStage,
        execution: Workflow,  # noqa: F821
        source_stage: StageExecution,
    ) -> bool:
        """Check if jump count is within limits. Returns False if exceeded."""
        from stabilize.models.workflow import Workflow  # noqa: F401

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
            self.retry_on_concurrency_error(
                partial(
                    self._reload_reset_and_store,
                    source_stage.id,
                    context_updates,
                    WorkflowStatus.TERMINAL,
                ),
                f"storing max-jump stage {source_stage.ref_id}",
            )

            self.txn_helper.execute_atomic(
                stage=None,
                source_message=message,
                messages_to_push=[
                    (
                        CompleteStage(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                            stage_id=source_stage.id,
                        ),
                        None,
                    )
                ],
                handler_name="JumpToStage",
            )
            return False
        return True

    def _reset_downstream_stages(
        self,
        message: JumpToStage,
        execution: Workflow,  # noqa: F821
        source_stage: StageExecution,
        target_stage: StageExecution,
    ) -> None:
        """Reset all downstream stages that depend on target."""
        downstream_stages = get_downstream_stages(execution, target_stage.ref_id)
        for stage in downstream_stages:
            if stage.id != source_stage.id:
                logger.debug("Resetting downstream stage: %s", stage.ref_id)
                self.retry_on_concurrency_error(
                    partial(self._reload_reset_and_store, stage.id),
                    f"storing downstream stage {stage.ref_id}",
                )
                self._reset_synthetics_for_stage(message.execution_id, stage.id)

    def _handle_source_stage(
        self,
        message: JumpToStage,
        execution: Workflow,  # noqa: F821
        source_stage: StageExecution,
        target_stage: StageExecution,
        is_backward_jump: bool,
    ) -> None:
        """Handle source stage based on jump direction."""
        if is_backward_jump:
            # Backward jump (retry loop): reset source so it can run again
            reset_stage_for_retry(source_stage)
        else:
            # Forward jump (skip ahead): mark source as succeeded
            end_time = self.current_time_millis()
            reset_stage_to_succeeded(source_stage, end_time)

            # Mark all stages between source and target as SKIPPED
            skipped_stages = get_skipped_stages(execution, source_stage, target_stage)
            for stage in skipped_stages:
                if stage.status == WorkflowStatus.NOT_STARTED:
                    logger.debug("Marking skipped stage: %s", stage.ref_id)
                    self.retry_on_concurrency_error(
                        partial(
                            self._reload_reset_and_store,
                            stage.id,
                            None,
                            WorkflowStatus.SKIPPED,
                        ),
                        f"storing skipped stage {stage.ref_id}",
                    )
