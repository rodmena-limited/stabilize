"""
StartStageHandler - handles stage startup.

This is one of the most critical handlers in the execution engine.
It checks if upstream stages are complete, plans the stage's tasks
and synthetic stages, and starts execution.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteStage,
    CompleteWorkflow,
    SkipStage,
    StartStage,
    StartTask,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

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

    @property
    def message_type(self) -> type[StartStage]:
        return StartStage

    def handle(self, message: StartStage) -> None:
        """Handle the StartStage message."""

        def on_stage(stage: StageExecution) -> None:
            try:
                # Check if upstream stages have failed
                if stage.any_upstream_stages_failed():
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
                if stage.all_upstream_stages_complete():
                    self._start_if_ready(stage, message)
                else:
                    # Upstream not complete - re-queue
                    logger.debug(
                        "Re-queuing %s (%s) - upstream stages not complete",
                        stage.name,
                        stage.id,
                    )
                    self.queue.push(message, self.retry_delay)

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

        # Check if should skip
        if self._should_skip(stage):
            logger.info("Skipping optional stage %s", stage.name)
            self.queue.push(
                SkipStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                )
            )
            return

        # Check if start time expired
        if self._is_after_start_time_expiry(stage):
            logger.warning("Stage %s start time expired, skipping", stage.name)
            self.queue.push(
                SkipStage(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                )
            )
            return

        # Plan and start the stage
        stage.start_time = self.current_time_millis()
        self._plan_stage(stage)
        stage.status = WorkflowStatus.RUNNING
        self.repository.store_stage(stage)

        self._start_stage(stage, message)

        logger.info("Started stage %s (%s)", stage.name, stage.id)

    def _plan_stage(self, stage: StageExecution) -> None:
        """
        Plan the stage - build tasks and before stages.

        This is where StageDefinitionBuilder.buildTasks() and
        buildBeforeStages() would be called.

        For now, we assume tasks are already defined on the stage.
        """
        # Mark first and last tasks
        if stage.tasks:
            stage.tasks[0].stage_start = True
            stage.tasks[-1].stage_end = True

        # TODO: Call stage definition builder to:
        # 1. Build tasks
        # 2. Build before stages
        # 3. Add context flags

    def _start_stage(
        self,
        stage: StageExecution,
        message: StartStage,
    ) -> None:
        """
        Start stage execution.

        Order of execution:
        1. Before stages (if any)
        2. Tasks (if any)
        3. After stages (if any)
        4. Complete stage
        """
        # Check for before stages
        before_stages = stage.first_before_stages()
        if before_stages:
            for before in before_stages:
                self.queue.push(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=before.id,
                    )
                )
            return

        # No before stages - start first task
        first_task = stage.first_task()
        if first_task:
            self.queue.push(
                StartTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=first_task.id,
                )
            )
            return

        # No tasks - check for after stages
        after_stages = stage.first_after_stages()
        if after_stages:
            for after in after_stages:
                self.queue.push(
                    StartStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=after.id,
                    )
                )
            return

        # No tasks or synthetic stages - complete immediately
        self.queue.push(
            CompleteStage(
                execution_type=message.execution_type,
                execution_id=message.execution_id,
                stage_id=message.stage_id,
            )
        )

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
