"""Task hooks that run once a stage's live tasks were cancelled and the cancellation committed."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from stabilize.handlers.run_task.execution import resolve_task

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.tasks.registry import TaskRegistry

logger = logging.getLogger(__name__)


def run_cancel_hooks(task_registry: TaskRegistry | None, stage: StageExecution, moved: list[TaskExecution]) -> None:
    """on_cancel for the tasks that were running, on_cleanup for every task; failures are logged, never raised."""
    if task_registry is None:
        if moved:
            logger.warning("No task registry; on_cancel/on_cleanup not called for %s", stage.name)
        return
    moved_ids = {t.id for t in moved}
    for task in stage.tasks:
        try:
            impl = resolve_task(task.implementing_class, task, task_registry)
        except Exception as e:
            logger.warning("Cannot resolve task %s for cancel hooks: %s", task.name, e)
            continue
        if task.id in moved_ids:
            try:
                impl.on_cancel(stage)
            except Exception as e:
                logger.warning("Error in on_cancel for task %s in stage %s: %s", task.name, stage.name, e)
        try:
            impl.on_cleanup(stage)
        except Exception as e:
            logger.warning("Error in on_cleanup for task %s in stage %s: %s", task.name, stage.name, e)
