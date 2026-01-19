"""
StartTaskHandler - handles task startup.

This handler prepares a task for execution and triggers RunTask.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.handlers.base import StabilizeHandler
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteTask,
    RunTask,
    StartTask,
)
from stabilize.tasks.interface import SkippableTask
from stabilize.tasks.registry import TaskNotFoundError, TaskRegistry

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue

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

    def __init__(
        self,
        queue: Queue,
        repository: WorkflowStore,
        task_registry: TaskRegistry,
        retry_delay: timedelta = timedelta(seconds=15),
    ) -> None:
        super().__init__(queue, repository, retry_delay)
        self.task_registry = task_registry

    @property
    def message_type(self) -> type[StartTask]:
        return StartTask

    def handle(self, message: StartTask) -> None:
        """Handle the StartTask message."""

        def on_task(stage: StageExecution, task_model: TaskExecution) -> None:
            # Idempotency check - only start tasks that are NOT_STARTED
            if task_model.status != WorkflowStatus.NOT_STARTED:
                logger.debug(
                    "Ignoring StartTask for %s (%s) - already %s",
                    task_model.name,
                    task_model.id,
                    task_model.status,
                )
                return

            # Check if task should be skipped
            try:
                task_impl = self.task_registry.get(task_model.implementing_class)
                if isinstance(task_impl, SkippableTask) and not task_impl.is_enabled(stage):
                    logger.info("Skipping task %s (disabled)", task_model.name)

                    # Mark as skipped - use atomic transaction
                    self.set_task_status(task_model, WorkflowStatus.SKIPPED)
                    with self.repository.transaction(self.queue) as txn:
                        txn.store_stage(stage)
                        if message.message_id:
                            txn.mark_message_processed(
                                message_id=message.message_id,
                                handler_type="StartTask",
                                execution_id=message.execution_id,
                            )
                        txn.push_message(
                            CompleteTask(
                                execution_type=message.execution_type,
                                execution_id=message.execution_id,
                                stage_id=message.stage_id,
                                task_id=message.task_id,
                                status=WorkflowStatus.SKIPPED,
                            )
                        )
                    return
            except TaskNotFoundError:
                # If implementation not found, RunTaskHandler will handle the error.
                # We proceed to start it so the error is reported in the proper phase.
                pass

            # Update task status
            self.set_task_status(task_model, WorkflowStatus.RUNNING)
            task_model.start_time = self.current_time_millis()

            # Atomic: store stage + push RunTask together
            with self.repository.transaction(self.queue) as txn:
                txn.store_stage(stage)
                if message.message_id:
                    txn.mark_message_processed(
                        message_id=message.message_id,
                        handler_type="StartTask",
                        execution_id=message.execution_id,
                    )
                txn.push_message(
                    RunTask(
                        execution_type=message.execution_type,
                        execution_id=message.execution_id,
                        stage_id=message.stage_id,
                        task_id=message.task_id,
                        task_type=task_model.implementing_class,
                    )
                )

            logger.debug(
                "Started task %s (%s) in stage %s",
                task_model.name,
                task_model.id,
                stage.name,
            )

        self.with_task(message, on_task)
