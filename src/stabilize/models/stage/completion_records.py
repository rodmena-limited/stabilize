"""Engine records on a stage's context that let recovery rebuild a lost task message.

``_task_completion`` maps a task id to the completion its handler committed
(status, original status, the task's start_time), written in the same
transaction as the CompleteTask it pushed and removed by CompleteTask. A
recovery sweep that finds a RUNNING task with a matching record re-pushes that
CompleteTask instead of re-running the task.

``_gate_checked`` lists the tasks whose SkippableTask.is_enabled StartTask has
evaluated, so RunTask evaluates it only for tasks started without that check.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution

COMPLETION_KEY = "_task_completion"
GATE_KEY = "_gate_checked"


def record_completion(
    stage: StageExecution,
    task_id: str,
    status: WorkflowStatus,
    original_status: WorkflowStatus | None = None,
) -> None:
    task = next((t for t in stage.tasks if t.id == task_id), None)
    records = dict(stage.context.get(COMPLETION_KEY) or {})
    records[task_id] = {
        "status": status.name,
        "original_status": original_status.name if original_status is not None else None,
        "start_time": task.start_time if task is not None else None,
    }
    stage.context[COMPLETION_KEY] = records


def clear_completion(stage: StageExecution, task_id: str) -> None:
    records = stage.context.get(COMPLETION_KEY)
    if not records or task_id not in records:
        return
    remaining = {k: v for k, v in records.items() if k != task_id}
    if remaining:
        stage.context[COMPLETION_KEY] = remaining
    else:
        stage.context.pop(COMPLETION_KEY, None)


def recorded_completion(
    stage: StageExecution, task: TaskExecution
) -> tuple[WorkflowStatus, WorkflowStatus | None] | None:
    record: Any = (stage.context.get(COMPLETION_KEY) or {}).get(task.id)
    if not isinstance(record, dict) or record.get("start_time") != task.start_time:
        return None
    try:
        status = WorkflowStatus[record["status"]]
        original = record.get("original_status")
        return status, WorkflowStatus[original] if original else None
    except (KeyError, TypeError):
        return None


def mark_gate_checked(stage: StageExecution, task_id: str) -> None:
    checked = list(stage.context.get(GATE_KEY) or [])
    if task_id not in checked:
        checked.append(task_id)
    stage.context[GATE_KEY] = checked


def gate_checked(stage: StageExecution, task_id: str) -> bool:
    return task_id in (stage.context.get(GATE_KEY) or [])
