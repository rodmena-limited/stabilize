"""Stage reset utilities for jump, restart and re-arm operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.models.stage.engine_keys import SIGNAL_KEYS
from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.tasks.registry import TaskRegistry

STAGE_RESET_KEYS: tuple[str, ...] = (
    "_join_fired",
    "_completed_branches",
    "_activated_branches",
    "_pruned_from",
    "_pruned",
    "_mm_consumed",
    "_mm_trigger",
    "_mm_firings",
    "_task_completion",
    "_gate_checked",
    "_on_failure_planned",
)

TASK_RESET_KEYS: tuple[str, ...] = ("exception", "_paused_at", "_paused_ms", *sorted(SIGNAL_KEYS))


def reset_keys_for(implementing_class: str, task_registry: TaskRegistry | None) -> tuple[str, ...]:
    """The context keys a reset clears for a task class, from the registry or the built-in tasks."""
    if task_registry is not None:
        return task_registry.reset_context_keys_for(implementing_class)
    from stabilize.tasks.builtins import BUILTIN_ALIASES, BUILTIN_TASKS

    impl = BUILTIN_TASKS.get(BUILTIN_ALIASES.get(implementing_class, implementing_class))
    return tuple(impl.reset_context_keys) if impl is not None else ()


def reset_tasks(stage: StageExecution, task_registry: TaskRegistry | None = None) -> None:
    """Return every task of *stage* to NOT_STARTED in a new generation and clear what the old run left behind."""
    stage.scheduled_time = None
    for key in TASK_RESET_KEYS:
        stage.context.pop(key, None)
    for task in stage.tasks:
        task.status = WorkflowStatus.NOT_STARTED
        task.start_time = None
        task.end_time = None
        task.task_exception_details = {}
        task.generation += 1
        for key in reset_keys_for(task.implementing_class, task_registry):
            stage.context.pop(key, None)


def reset_stage_for_retry(stage: StageExecution, *, task_registry: TaskRegistry | None = None) -> None:
    """Reset a stage and its tasks to NOT_STARTED for retry.

    This modifies the stage in place. Used when reloading fresh from DB.
    """
    stage.status = WorkflowStatus.NOT_STARTED
    stage.start_time = None
    stage.end_time = None
    stage.outputs = {}
    for key in STAGE_RESET_KEYS:
        stage.context.pop(key, None)
    reset_tasks(stage, task_registry)


def reset_stage_to_succeeded(stage: StageExecution, end_time: int) -> None:
    """Mark a stage as SUCCEEDED."""
    stage.status = WorkflowStatus.SUCCEEDED
    stage.end_time = end_time
    for task in stage.tasks:
        if task.status == WorkflowStatus.RUNNING:
            task.status = WorkflowStatus.SUCCEEDED
            task.end_time = end_time


def reset_stage_to_skipped(stage: StageExecution, end_time: int) -> None:
    """Mark a stage as SKIPPED."""
    stage.status = WorkflowStatus.SKIPPED
    stage.end_time = end_time
    for task in stage.tasks:
        task.status = WorkflowStatus.SKIPPED
        task.end_time = end_time


def reset_stage_to_terminal(stage: StageExecution, end_time: int) -> None:
    """Mark a stage as TERMINAL."""
    stage.status = WorkflowStatus.TERMINAL
    stage.end_time = end_time
    for task in stage.tasks:
        if task.status == WorkflowStatus.RUNNING:
            task.status = WorkflowStatus.TERMINAL
            task.end_time = end_time
