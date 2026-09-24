"""The persisted state of a task row, for deciding whether a stage write must send it."""

from __future__ import annotations

import json
from typing import Any

from stabilize.models.task import TaskExecution


def _state(task: TaskExecution) -> tuple[Any, ...]:
    details: object
    try:
        details = json.dumps(task.task_exception_details, sort_keys=True, default=str)
    except (TypeError, ValueError):
        details = object()
    return (
        task.id,
        task.version,
        task.name,
        task.implementing_class,
        task.status.name,
        task.start_time,
        task.end_time,
        task.stage_start,
        task.stage_end,
        task.loop_start,
        task.loop_end,
        details,
    )


def mark_persisted(task: TaskExecution) -> None:
    """Record the task's current state as the state of its row."""
    task._persisted_state = _state(task)


def needs_write(task: TaskExecution) -> bool:
    """True unless the task is known to match its row exactly."""
    return task._persisted_state is None or task._persisted_state != _state(task)


Captured = list[tuple[TaskExecution, tuple[Any, ...]]]


def capture(tasks: list[TaskExecution]) -> Captured:
    """The state each task was written with, to record once the write commits."""
    return [(task, _state(task)) for task in tasks]


def commit_captured(captured: Captured) -> None:
    for task, state in captured:
        task._persisted_state = state


def versions(tasks: list[TaskExecution]) -> list[tuple[TaskExecution, int]]:
    """Each task with its current version, to restore if the write rolls back."""
    return [(task, task.version) for task in tasks]


def restore_versions(saved: list[tuple[TaskExecution, int]]) -> None:
    for task, version in saved:
        task.version = version
