"""Task system for pipeline execution."""

from __future__ import annotations

from stabilize.tasks.interface import (
    CallableTask,
    RetryableTask,
    SkippableTask,
    Task,
)
from stabilize.tasks.registry import TaskRegistry
from stabilize.tasks.result import TaskResult

__all__ = [
    "CallableTask",
    "RetryableTask",
    "SkippableTask",
    "Task",
    "TaskRegistry",
    "TaskResult",
]
