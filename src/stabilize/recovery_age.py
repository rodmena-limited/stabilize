"""How old a workflow is for recovery: the newest thing any of its rows records."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow


def workflow_activity_ms(workflow: Workflow, store: Any) -> int | None:
    """The epoch millisecond of the workflow's latest activity, or None when nothing records a time."""
    candidates: list[int] = []

    def add(value: Any) -> None:
        if isinstance(value, int) and not isinstance(value, bool):
            candidates.append(value)

    add(workflow.stabilize_updated_at)
    add(workflow.created_at_ms)
    add(workflow.start_time)
    add(workflow.end_time)
    for stage in workflow.stages:
        add(stage.stabilize_updated_at)
        add(stage.start_time)
        add(stage.end_time)
        add(stage.scheduled_time)
        for task in stage.tasks:
            add(task.stabilize_updated_at)
            add(task.start_time)
            add(task.end_time)
    latest_signal = getattr(store, "pending_signal_latest_ms", None)
    if callable(latest_signal):
        add(latest_signal(workflow.id))
    return max(candidates) if candidates else None
