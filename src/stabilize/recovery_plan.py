"""The messages one recovery sweep re-issues for one RUNNING workflow.

Each rule rebuilds the message the normal path would have pushed from what the
store already records, and only when no queued message (including one being
handled) names the task, the stage or the workflow.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.models.stage.completion_records import recorded_completion
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import (
    CompleteStage,
    CompleteTask,
    CompleteWorkflow,
    RunTask,
    SignalStage,
    StartStage,
    StartTask,
)
from stabilize.recovery_rules import can_start, has_started

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message

ACTIVE = frozenset({WorkflowStatus.RUNNING, WorkflowStatus.PAUSED, WorkflowStatus.SUSPENDED, WorkflowStatus.BUFFERED})


def plan_running_workflow(workflow: Workflow, queue: Queue, store: Any | None = None) -> list[Message]:
    messages: list[Message] = []
    for stage in workflow.stages:
        if stage.status == WorkflowStatus.RUNNING:
            messages.extend(_plan_running_stage(workflow, stage, queue))
        elif stage.status == WorkflowStatus.SUSPENDED and store is not None:
            messages.extend(_plan_suspended_stage(workflow, stage, queue, store))
        elif (
            stage.status == WorkflowStatus.NOT_STARTED
            and (has_started(stage) or can_start(stage, workflow))
            and not queue.has_pending_message_for_stage(stage.id)
        ):
            messages.append(StartStage(execution_type=workflow.type.value, execution_id=workflow.id, stage_id=stage.id))
    if not messages and _needs_completion(workflow) and not queue.has_pending_message_for_execution(workflow.id):
        messages.append(CompleteWorkflow(execution_type=workflow.type.value, execution_id=workflow.id))
    return messages


def _plan_running_stage(workflow: Workflow, stage: StageExecution, queue: Queue) -> list[Message]:
    kind = workflow.type.value
    running = [t for t in stage.tasks if t.status == WorkflowStatus.RUNNING]
    not_started = [t for t in stage.tasks if t.status == WorkflowStatus.NOT_STARTED]
    if running:
        out: list[Message] = []
        for task in running:
            if queue.has_pending_message_for_task(task.id):
                continue
            recorded = recorded_completion(stage, task)
            if recorded is not None:
                status, original = recorded
                out.append(
                    CompleteTask(
                        execution_type=kind,
                        execution_id=workflow.id,
                        stage_id=stage.id,
                        task_id=task.id,
                        generation=task.generation,
                        status=status,
                        original_status=original,
                        task_start_time=task.start_time,
                    )
                )
            else:
                out.append(
                    RunTask(
                        execution_type=kind,
                        execution_id=workflow.id,
                        stage_id=stage.id,
                        task_id=task.id,
                        generation=task.generation,
                        task_type=task.implementing_class,
                    )
                )
        return out
    if not_started and stage.start_time is not None:
        if not all(s.status.is_complete for s in stage.before_stages()):
            return []
        first = not_started[0]
        if queue.has_pending_message_for_task(first.id):
            return []
        return [
            StartTask(
                execution_type=kind,
                execution_id=workflow.id,
                stage_id=stage.id,
                task_id=first.id,
                generation=first.generation,
            )
        ]
    if stage.tasks and all(t.status.is_complete for t in stage.tasks):
        if queue.has_pending_message_for_stage(stage.id) or any(s.status in ACTIVE for s in stage.after_stages()):
            return []
        return [CompleteStage(execution_type=kind, execution_id=workflow.id, stage_id=stage.id)]
    if queue.has_pending_message_for_stage(stage.id):
        return []
    return [StartStage(execution_type=kind, execution_id=workflow.id, stage_id=stage.id)]


def _plan_suspended_stage(workflow: Workflow, stage: StageExecution, queue: Queue, store: Any) -> list[Message]:
    """A SignalStage for the oldest unconsumed signal row of a suspended stage nothing is about to deliver to."""
    pending = getattr(store, "pending_signals", None)
    if not callable(pending) or queue.has_pending_message_for_stage(stage.id):
        return []
    rows = pending(workflow.id, stage.ref_id)
    return [
        SignalStage(
            execution_type=workflow.type.value,
            execution_id=workflow.id,
            stage_id=stage.id,
            signal_name=row["signal_name"],
            signal_data=row["signal_data"],
            persistent=True,
            user=row.get("user", ""),
            signal_row_id=row["id"],
        )
        for row in rows[:1]
    ]


def _needs_completion(workflow: Workflow) -> bool:
    top = workflow.top_level_stages()
    if not top or any(s.status in ACTIVE for s in top):
        return False
    return not any(s.status == WorkflowStatus.NOT_STARTED and can_start(s, workflow) for s in top)
