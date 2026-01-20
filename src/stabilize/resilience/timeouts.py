"""
Centralized timeout management.

Provides consistent timeout calculation logic for tasks and stages.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.tasks.interface import Task


class TimeoutManager:
    """Manages timeout calculations."""

    def __init__(self, default_task_timeout_seconds: float = 300.0) -> None:
        self.default_task_timeout_seconds = default_task_timeout_seconds

    def get_task_timeout(self, stage: StageExecution, task: Task) -> timedelta:
        """
        Get the effective timeout for a task.

        Priority:
        1. Task-specific dynamic timeout (if RetryableTask)
        2. Stage-level override (if supported by task or generally applicable)
        3. Global default
        """
        # 1. Task-specific dynamic timeout (RetryableTask)
        if hasattr(task, "get_dynamic_timeout"):
            # RetryableTask.get_dynamic_timeout usually checks stage context too
            # We assume it's the source of truth if present
            # But we need to cast to RetryableTask to be safe or just call it
            from stabilize.tasks.interface import RetryableTask

            if isinstance(task, RetryableTask):
                return task.get_dynamic_timeout(stage)
            # Fallback if hasattr but not instance (duck typing)
            return getattr(task, "get_dynamic_timeout")(stage)  # type: ignore[no-any-return]

        # 2. Stage-level override (standard context key)
        # Some tasks might not be RetryableTask but still want to respect this
        stage_timeout_ms = stage.context.get("stageTimeoutMs")
        if stage_timeout_ms is not None and isinstance(stage_timeout_ms, (int, float)):
            return timedelta(milliseconds=stage_timeout_ms)

        # 3. Global default
        return timedelta(seconds=self.default_task_timeout_seconds)
