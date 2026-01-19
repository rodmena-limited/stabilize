"""
StartStageHandler - handles stage startup.

This is one of the most critical handlers in the execution engine.
It checks if upstream stages are complete, plans the stage's tasks
and synthetic stages, and starts execution.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.dag.graph import StageGraphBuilder
from stabilize.handlers.base import StabilizeHandler
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteStage,
    CompleteWorkflow,
    SkipStage,
    StartStage,
    StartTask,
)
from stabilize.stages.builder import get_default_factory

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

logger = logging.getLogger(__name__)

# Maximum number of times to re-queue StartStage before giving up
# With a 15-second retry delay, 240 retries = 1 hour maximum wait
MAX_START_STAGE_RETRIES = 240


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

                # Check if any upstream stages failed
                # We check for terminal statuses in direct dependencies
                halt_statuses = {
                    WorkflowStatus.TERMINAL,
                    WorkflowStatus.STOPPED,
                    WorkflowStatus.CANCELED,
                }

                upstream_failed = False
                for upstream in upstream_stages:
                    if upstream is None:
                        continue
                    if upstream.status in halt_statuses:
                        upstream_failed = True
                        break

                if upstream_failed:
                    logger.warning(
                        "Upstream stage failed for %s (%s), completing execution",
                        stage.name,
                        stage.id,
                    )
                    self.queue.push(
                        CompleteWorkflow(
                            execution_type=message.execution_type,
                            execution_id=message.execution_id,
                        )
                    )
                    return

                # Check if all upstream stages are complete
                # CONTINUABLE_STATUSES = {SUCCEEDED, FAILED_CONTINUE, SKIPPED}
                # We can't import CONTINUABLE_STATUSES easily inside here without circular imports potentially,
                # but it is available in models.status
                from stabilize.models.status import CONTINUABLE_STATUSES

                all_complete = True
                for upstream in upstream_stages:
                    if upstream is None:
                        continue
                    if upstream.status not in CONTINUABLE_STATUSES:
                        all_complete = False
                        break

                if all_complete:
                    self._start_if_ready(stage, message)
                else:
                    # Upstream not complete - check retry count before re-queuing
                    retry_count = getattr(message, "retry_count", 0) or 0

                    if retry_count >= MAX_START_STAGE_RETRIES:
                        logger.error(
                            "StartStage for %s (%s) exceeded max retries (%d). "
                            "Upstream stages may be stuck. Marking as TERMINAL.",
                            stage.name,
                            stage.id,
                            MAX_START_STAGE_RETRIES,
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
                        "Re-queuing %s (%s) (retry %d/%d) - upstream stages not complete",
                        stage.name,
                        stage.id,
                        retry_count + 1,
                        MAX_START_STAGE_RETRIES,
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
                logger.error(
                    "Error starting stage %s (%s): %s",
                    stage.name,
                    stage.id,
                    e,
                    exc_info=True,
                )
                stage.context["exception"] = {
                    "details": {"error": str(e)},
                }
                stage.context["beforeStagePlanningFailed"] = True
                self.repository.store_stage(stage)
                self.queue.push(
                    CompleteStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                    )
                )

        self.with_stage(message, on_stage)

    def _start_if_ready(
        self,
        stage: StageExecution,
        message: StartStage,
    ) -> None:
        """Start the stage if it's ready to run."""
        # Check if already processed
        if stage.status != WorkflowStatus.NOT_STARTED:
            logger.warning(
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

        # Plan the stage (this modifies stage state but doesn't persist yet)
        stage.start_time = self.current_time_millis()
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
        self.set_stage_status(stage, WorkflowStatus.RUNNING)

        # Collect messages to push BEFORE starting the transaction
        messages_to_push = self._collect_start_messages(stage, message)

        # Atomic: store stage + push all start messages together
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

        logger.info("Started stage %s (%s)", stage.name, stage.id)

    def _collect_start_messages(
        self,
        stage: StageExecution,
        message: StartStage,
    ) -> list:
        """Collect messages needed to start the stage.

        This method queries the repository but doesn't push any messages.
        The caller is responsible for pushing the returned messages atomically.
        """
        from stabilize.queue.messages import Message

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
