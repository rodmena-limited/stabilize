"""
StartTaskHandler - handles task startup.

This handler prepares a task for execution and triggers RunTask.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    RunTask,
    StartTask,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution

logger = logging.getLogger(__name__)


class StartTaskHandler(StabilizeHandler[StartTask]):
    """
    Handler for StartTask messages.

    Execution flow:
    1. Check if task is enabled (SkippableTask)
       - If not: Push CompleteTask(SKIPPED)
    2. Set task status to RUNNING
    3. Set task start time
    4. Push RunTask
    """

    @property
    def message_type(self) -> type[StartTask]:
        return StartTask

    def handle(self, message: StartTask) -> None:
        """Handle the StartTask message."""

        def on_task(stage: StageExecution, task: TaskExecution) -> None:
            # Check if task should be skipped
            # TODO: Check SkippableTask.isEnabled()

            # Update task status
            task.status = WorkflowStatus.RUNNING
            task.start_time = self.current_time_millis()

            # Save the stage (which includes the task)
            self.repository.store_stage(stage)

            # Push RunTask
            self.queue.push(
                RunTask(
                    execution_type=message.execution_type,
                    execution_id=message.execution_id,
                    stage_id=message.stage_id,
                    task_id=message.task_id,
                    task_type=task.implementing_class,
                )
            )

            logger.debug(
                "Started task %s (%s) in stage %s",
                task.name,
                task.id,
                stage.name,
            )

        self.with_task(message, on_task)
