"""
CompleteTaskHandler - handles task completion.

This handler updates task status and triggers either the next task
or stage completion.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.queue.messages import (
    CompleteStage,
    CompleteTask,
    StartTask,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution

logger = logging.getLogger(__name__)


class CompleteTaskHandler(StabilizeHandler[CompleteTask]):
    """
    Handler for CompleteTask messages.

    Execution flow:
    1. Update task status and end time
    2. If there's a next task: Push StartTask
    3. Otherwise: Push CompleteStage
    """

    @property
    def message_type(self) -> type[CompleteTask]:
        return CompleteTask

    def handle(self, message: CompleteTask) -> None:
        """Handle the CompleteTask message."""

        def on_task(stage: StageExecution, task: TaskExecution) -> None:
            # Update task status
            task.status = message.status
            task.end_time = self.current_time_millis()

            # Save the stage
            self.repository.store_stage(stage)

            logger.debug("Task %s completed with status %s", task.name, message.status)

            # Check for next task
            next_task = stage.next_task(task)
            if next_task is not None:
                self.queue.push(
                    StartTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=next_task.id,
                    )
                )
            else:
                # No more tasks - complete stage
                self.queue.push(
                    CompleteStage(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                    )
                )

        self.with_task(message, on_task)
