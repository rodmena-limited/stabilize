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
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
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
    from stabilize.queue.queue import Queue

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
                logger.error(
                    "Jump target stage not found: %s (execution: %s)",
                    message.target_stage_ref_id,
                    message.execution_id,
                )
                # Mark stage as terminal and complete it
                source_stage.context["jump_error"] = f"Target stage not found: {message.target_stage_ref_id}"
                source_stage.status = WorkflowStatus.TERMINAL
                source_stage.end_time = self.current_time_millis()
                self.txn_helper.execute_atomic(
                    stage=source_stage,
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
                return

            # Check jump count to prevent infinite loops
            # Jump count is stored in source stage context
            jump_count = source_stage.context.get("_jump_count", 0)
            # Max jumps can be set in execution context (at workflow creation)
            # or in source stage context
            max_jumps = (
                execution.context.get("_max_jumps") or source_stage.context.get("_max_jumps") or DEFAULT_MAX_JUMPS
            )

            if jump_count >= max_jumps:
                logger.error(
                    "Max jump count exceeded (%d) for execution %s",
                    max_jumps,
                    message.execution_id,
                )
                source_stage.context["jump_error"] = f"Max jump count exceeded: {jump_count}/{max_jumps}"
                # Mark stage as TERMINAL so workflow completes
                source_stage.status = WorkflowStatus.TERMINAL
                source_stage.end_time = self.current_time_millis()
                # Use CompleteStage to properly handle stage completion flow
                # which will then trigger CompleteWorkflow
                self.txn_helper.execute_atomic(
                    stage=source_stage,
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
                return

            # Reset target stage
            target_stage.status = WorkflowStatus.NOT_STARTED
            target_stage.start_time = None
            target_stage.end_time = None
            # Reset all tasks to NOT_STARTED so they run again
            for task in target_stage.tasks:
                task.status = WorkflowStatus.NOT_STARTED
                task.start_time = None
                task.end_time = None
                task.task_exception_details = {}

            # Merge jump context into target stage
            if message.jump_context:
                target_stage.context.update(message.jump_context)

            # Make jump outputs available via special context key
            if message.jump_outputs:
                target_stage.context["_jump_outputs"] = message.jump_outputs

            # Set bypass flag so StartStageHandler skips prerequisite checks
            target_stage.context["_jump_bypass"] = True

            # Increment jump counter (stored in source stage context since
            # execution context storage is limited)
            new_jump_count = jump_count + 1
            source_stage.context["_jump_count"] = new_jump_count

            # Record jump history for debugging in source stage context
            jump_history = source_stage.context.get("_jump_history", [])
            jump_history.append(
                {
                    "from_stage": source_stage.ref_id,
                    "to_stage": message.target_stage_ref_id,
                    "jump_number": new_jump_count,
                    "context_keys": list(message.jump_context.keys()) if message.jump_context else [],
                }
            )
            source_stage.context["_jump_history"] = jump_history

            # Also store in target stage for access by subsequent tasks
            target_stage.context["_jump_count"] = new_jump_count
            target_stage.context["_jump_history"] = jump_history

            logger.info(
                "Jumping from stage %s to %s (jump #%d/%d)",
                source_stage.ref_id,
                message.target_stage_ref_id,
                new_jump_count,
                max_jumps,
            )

            # When jumping to a different stage, store source separately
            # When jumping to same stage (self-loop), they're the same object
            is_self_loop = source_stage.id == target_stage.id
            if not is_self_loop:
                self.repository.store_stage(source_stage)

            self.txn_helper.execute_atomic(
                stage=target_stage,
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
