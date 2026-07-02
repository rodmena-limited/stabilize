"""Stage reset utilities for jump operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution


def reset_stage_for_retry(stage: StageExecution) -> None:
    """Reset a stage and its tasks to NOT_STARTED for retry.

    This modifies the stage in place. Used when reloading fresh from DB.
    """
    stage.status = WorkflowStatus.NOT_STARTED
    stage.start_time = None
    stage.end_time = None
    stage.outputs = {}
    # Re-arm join/split tracking: a fired discriminator or N-of-M join would
    # otherwise never become READY again inside a jump_to retry loop, and a
    # split's recorded branch activations belong to the previous iteration.
    for key in ("_join_fired", "_completed_branches", "_activated_branches"):
        stage.context.pop(key, None)
    for task in stage.tasks:
        task.status = WorkflowStatus.NOT_STARTED
        task.start_time = None
        task.end_time = None
        task.task_exception_details = {}


def reset_stage_to_succeeded(stage: StageExecution, end_time: int) -> None:
    """Mark a stage as SUCCEEDED.

    Args:
        stage: The stage to mark as succeeded
        end_time: The end time in milliseconds
    """
    stage.status = WorkflowStatus.SUCCEEDED
    stage.end_time = end_time
    for task in stage.tasks:
        if task.status == WorkflowStatus.RUNNING:
            task.status = WorkflowStatus.SUCCEEDED
            task.end_time = end_time


def reset_stage_to_skipped(stage: StageExecution, end_time: int) -> None:
    """Mark a stage as SKIPPED.

    Args:
        stage: The stage to mark as skipped
        end_time: The end time in milliseconds
    """
    stage.status = WorkflowStatus.SKIPPED
    stage.end_time = end_time
    for task in stage.tasks:
        task.status = WorkflowStatus.SKIPPED
        task.end_time = end_time


def reset_stage_to_terminal(stage: StageExecution, end_time: int) -> None:
    """Mark a stage as TERMINAL.

    Args:
        stage: The stage to mark as terminal
        end_time: The end time in milliseconds
    """
    stage.status = WorkflowStatus.TERMINAL
    stage.end_time = end_time
    for task in stage.tasks:
        if task.status == WorkflowStatus.RUNNING:
            task.status = WorkflowStatus.TERMINAL
            task.end_time = end_time
