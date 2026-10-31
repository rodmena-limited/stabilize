"""Saving a completed task's result, and what happens when the save fails."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from stabilize.errors.delivery import DuplicateDeliveryError
from stabilize.handlers.run_task import held_results
from stabilize.handlers.run_task.error import complete_with_error
from stabilize.handlers.run_task.generation import same_generation
from stabilize.handlers.run_task.result import get_backoff_period, process_result
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.tenancy import TenancyModeChanged, TenantContextError, TenantMismatchError

if TYPE_CHECKING:
    from datetime import timedelta

    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.queue.messages import RunTask
    from stabilize.tasks.result import TaskResult

logger = logging.getLogger(__name__)


def save_or_hold(handler: Any, task_model: TaskExecution, result: TaskResult, message: RunTask) -> None:
    """Save result. If the store is unavailable, hold it and re-raise for the redelivery.

    If the store refuses the result itself, the task completes TERMINAL with the reason,
    because a redelivery would be refused the same way.
    """
    try:
        handler._process_result_safely(message.stage_id, task_model.id, result, message)
    except (TenancyModeChanged, TenantMismatchError, TenantContextError, DuplicateDeliveryError):
        raise
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


class RunTaskSavingMixin:
    """How the RunTask handler saves a result: the retrying reload, the drop rules, the backoff."""

    repository: Any
    txn_helper: Any
    task_registry: Any
    _task_backoff: Any

    def retry_on_concurrency_error(self, func: Any, context: str = "operation") -> None:
        raise NotImplementedError

    def current_time_millis(self) -> int:
        raise NotImplementedError

    def _save_result(self, task_model: TaskExecution, result: TaskResult, message: RunTask) -> None:
        save_or_hold(self, task_model, result, message)

    def _process_result_safely(
        self,
        stage_id: str,
        task_id: str,
        result: TaskResult,
        message: RunTask,
    ) -> None:
        """Process result with retry on concurrency error."""

        def do_process() -> None:
            # Always reload stage to get latest version
            stage = self.repository.retrieve_stage(stage_id)
            if not stage:
                logger.error("Stage %s not found processing result", stage_id)
                return

            # Find task model
            task_model = next((t for t in stage.tasks if t.id == task_id), None)
            if not task_model:
                logger.error("Task %s not found in stage %s", task_id, stage_id)
                return
            if task_model.status != WorkflowStatus.RUNNING:
                logger.warning(
                    "Dropping the result of task %s; the task is %s, not RUNNING",
                    task_model.name,
                    task_model.status.name,
                )
                self.txn_helper.execute_atomic(source_message=message, handler_name="RunTask")
                return
            if not same_generation(task_model, message):
                logger.warning(
                    "Dropping the result of task %s generation %s; the task is at generation %s",
                    task_model.name,
                    message.generation,
                    task_model.generation,
                )
                self.txn_helper.execute_atomic(source_message=message, handler_name="RunTask")
                return

            process_result(
                stage,
                task_model,
                result,
                message,
                self.txn_helper,
                self._get_backoff_period,
            )

        self.retry_on_concurrency_error(do_process, f"processing result for task {task_id}")

    def _get_backoff_period(
        self,
        stage: StageExecution,
        task_model: TaskExecution,
        message: RunTask,
        attempt: int = 1,
    ) -> timedelta:
        """Calculate backoff period for retry."""
        return get_backoff_period(
            stage,
            task_model,
            message,
            attempt,
            self.task_registry,
            self._task_backoff,
            self.current_time_millis,
        )
