"""Saving a completed task's result, and what happens when the save fails."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.run_task import held_results
from stabilize.handlers.run_task.error import complete_with_error

if TYPE_CHECKING:
    from stabilize.handlers.run_task.handler import RunTaskHandler
    from stabilize.models.task import TaskExecution
    from stabilize.queue.messages import RunTask
    from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)


def save_or_hold(handler: RunTaskHandler, task_model: TaskExecution, result: TaskResult, message: RunTask) -> None:
    """Save result. If the store is unavailable, hold it and re-raise for the redelivery.

    If the store refuses the result itself, the task completes TERMINAL with the reason,
    because a redelivery would be refused the same way.
    """
    try:
        handler._process_result_safely(message.stage_id, task_model.id, result, message)
    except Exception as e:
        if held_results.is_store_unavailable(e):
            held_results.hold(task_model, result)
            logger.error(
                "Task %s completed but its result could not be saved; holding it for the redelivery",
                task_model.id,
                exc_info=True,
            )
            raise
        logger.error("Task %s completed but its result cannot be stored: %s", task_model.name, e)
        stage = handler.repository.retrieve_stage(message.stage_id)
        complete_with_error(
            stage,
            task_model,
            message,
            f"Task result could not be stored: {type(e).__name__}: {e}",
            handler.repository,
            handler.txn_helper,
            handler.retry_on_concurrency_error,
        )
