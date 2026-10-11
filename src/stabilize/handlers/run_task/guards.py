"""RunTask steps around a task's own code: its timeout hook, its skip gate, its BaseExceptions."""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from stabilize.handlers.run_task.error import complete_with_error
from stabilize.models.stage.completion_records import GATE_KEY, gate_checked
from stabilize.tasks.interface import SkippableTask
from stabilize.tasks.result import TaskResult

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.queue.messages import RunTask
    from stabilize.tasks.interface import Task

logger = logging.getLogger(__name__)

CONVERTED_BASE_EXCEPTIONS = (SystemExit, GeneratorExit)


class TaskBaseExceptionError(Exception):
    """A task raised a BaseException that is not an Exception."""


def complete_timed_out(
    handler: Any,
    stage: StageExecution,
    task_model: TaskExecution,
    task: Task,
    message: RunTask,
    reason: str,
) -> None:
    hook = getattr(task, "on_timeout", None)
    result: TaskResult | None = None
    if hook is not None:
        try:
            result = hook(stage)
        except Exception as exc:
            logger.error("on_timeout raised for task %s: %s", task_model.name, exc)
            reason = f"{reason}; on_timeout raised {type(exc).__name__}: {exc}"
            result = None
    if result is None:
        complete_with_error(
            stage,
            task_model,
            message,
            reason,
            handler.repository,
            handler.txn_helper,
            handler.retry_on_concurrency_error,
        )
    else:
        handler._save_result(task_model, result, message)


def ungated_skip(task: Task, stage: StageExecution, task_model: TaskExecution) -> TaskResult | None:
    """Evaluate is_enabled for a SkippableTask that StartTask did not gate (started by 0.32.x)."""
    if not isinstance(task, SkippableTask) or gate_checked(stage, task_model.id):
        return None
    if task.is_enabled(stage):
        return None
    return TaskResult.skipped()


def with_gate_record(result: TaskResult, task: Task, stage: StageExecution, task_model: TaskExecution) -> TaskResult:
    if not isinstance(task, SkippableTask) or gate_checked(stage, task_model.id):
        return result
    checked = list(stage.context.get(GATE_KEY) or [])
    checked.append(task_model.id)
    return replace(result, context={**(result.context or {}), GATE_KEY: checked})


def as_task_error(exc: BaseException) -> TaskBaseExceptionError:
    return TaskBaseExceptionError(f"task raised {type(exc).__name__}: {exc}")
