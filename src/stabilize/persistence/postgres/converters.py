"""Row conversion utilities for PostgreSQL persistence."""

from __future__ import annotations

import json
from typing import Any

from stabilize.models.stage import StageExecution, SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import (
    PausedDetails,
    Trigger,
    Workflow,
    WorkflowType,
)


def execution_to_dict(execution: Workflow) -> dict[str, Any]:
    """Convert execution to dictionary for storage."""
    return {
        "id": execution.id,
        "type": execution.type.value,
        "application": execution.application,
        "name": execution.name,
        "status": execution.status.name,
        "start_time": execution.start_time,
        "end_time": execution.end_time,
        "start_time_expiry": execution.start_time_expiry,
        "trigger": json.dumps(execution.trigger.to_dict()),
        "is_canceled": execution.is_canceled,
        "canceled_by": execution.canceled_by,
        "cancellation_reason": execution.cancellation_reason,
        "paused": (json.dumps(paused_to_dict(execution.paused)) if execution.paused else None),
        "pipeline_config_id": execution.pipeline_config_id,
        "is_limit_concurrent": execution.is_limit_concurrent,
        "max_concurrent_executions": execution.max_concurrent_executions,
        "keep_waiting_pipelines": execution.keep_waiting_pipelines,
        "origin": execution.origin,
    }


def paused_to_dict(paused: PausedDetails | None) -> dict[str, Any] | None:
    """Convert PausedDetails to dict."""
    if paused is None:
        return None
    return {
        "paused_by": paused.paused_by,
        "pause_time": paused.pause_time,
        "resume_time": paused.resume_time,
        "paused_ms": paused.paused_ms,
    }


def row_to_execution(row: dict[str, Any]) -> Workflow:
    """Convert database row to Workflow."""
    trigger_data = row["trigger"] if isinstance(row["trigger"], dict) else json.loads(row["trigger"] or "{}")
    paused_data = (
        row["paused"] if isinstance(row["paused"], dict) else json.loads(row["paused"]) if row["paused"] else None
    )

    paused = None
    if paused_data:
        paused = PausedDetails(
            paused_by=paused_data.get("paused_by", ""),
            pause_time=paused_data.get("pause_time"),
            resume_time=paused_data.get("resume_time"),
            paused_ms=paused_data.get("paused_ms", 0),
        )

    return Workflow(
        id=row["id"],
        type=WorkflowType(row["type"]),
        application=row["application"],
        name=row["name"] or "",
        status=WorkflowStatus[row["status"]],
        start_time=row["start_time"],
        end_time=row["end_time"],
        start_time_expiry=row["start_time_expiry"],
        trigger=Trigger.from_dict(trigger_data),
        is_canceled=row["is_canceled"] or False,
        canceled_by=row["canceled_by"],
        cancellation_reason=row["cancellation_reason"],
        paused=paused,
        pipeline_config_id=row["pipeline_config_id"],
        is_limit_concurrent=row["is_limit_concurrent"] or False,
        max_concurrent_executions=row["max_concurrent_executions"] or 0,
        keep_waiting_pipelines=row["keep_waiting_pipelines"] or False,
        origin=row["origin"] or "unknown",
    )


def row_to_stage(row: dict[str, Any]) -> StageExecution:
    """Convert database row to StageExecution."""
    context = row["context"] if isinstance(row["context"], dict) else json.loads(row["context"] or "{}")
    outputs = row["outputs"] if isinstance(row["outputs"], dict) else json.loads(row["outputs"] or "{}")

    synthetic_owner = None
    if row["synthetic_stage_owner"]:
        synthetic_owner = SyntheticStageOwner(row["synthetic_stage_owner"])

    return StageExecution(
        id=row["id"],
        ref_id=row["ref_id"],
        type=row["type"],
        name=row["name"] or "",
        status=WorkflowStatus[row["status"]],
        context=context,
        outputs=outputs,
        requisite_stage_ref_ids=set(row["requisite_stage_ref_ids"] or []),
        parent_stage_id=row["parent_stage_id"],
        synthetic_stage_owner=synthetic_owner,
        start_time=row["start_time"],
        end_time=row["end_time"],
        start_time_expiry=row["start_time_expiry"],
        scheduled_time=row["scheduled_time"],
        version=row.get("version", 0) or 0,
    )


def row_to_task(row: dict[str, Any]) -> TaskExecution:
    """Convert database row to TaskExecution."""
    # Handle exception_details consistently with SQLite:
    # - None -> {}
    # - str -> parse as JSON
    # - dict -> use directly (JSONB auto-parsed)
    raw_exception = row["task_exception_details"]
    if raw_exception is None:
        exception_details: dict[str, Any] = {}
    elif isinstance(raw_exception, str):
        exception_details = json.loads(raw_exception) if raw_exception else {}
    else:
        # Already a dict (JSONB auto-parsed by psycopg)
        exception_details = raw_exception if raw_exception else {}

    return TaskExecution(
        id=row["id"],
        name=row["name"],
        implementing_class=row["implementing_class"],
        status=WorkflowStatus[row["status"]],
        start_time=row["start_time"],
        end_time=row["end_time"],
        stage_start=row["stage_start"] or False,
        stage_end=row["stage_end"] or False,
        loop_start=row["loop_start"] or False,
        loop_end=row["loop_end"] or False,
        task_exception_details=exception_details,
        version=row.get("version", 0) or 0,
    )
