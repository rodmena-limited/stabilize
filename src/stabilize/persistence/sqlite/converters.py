"""Row conversion utilities for SQLite persistence."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING, Any

from stabilize.models.stage import StageExecution, SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import (
    PausedDetails,
    Trigger,
    Workflow,
    WorkflowType,
)

if TYPE_CHECKING:
    pass


def execution_to_dict(execution: Workflow) -> dict[str, Any]:
    """Convert execution to dictionary for storage."""
    return {
        "id": execution.id,
        "type": execution.type.value,
        "application": execution.application,
        "name": execution.name,
        "status": execution.status.name,
        "context": json.dumps(execution.context),
        "start_time": execution.start_time,
        "end_time": execution.end_time,
        "start_time_expiry": execution.start_time_expiry,
        "trigger": json.dumps(execution.trigger.to_dict()),
        "is_canceled": 1 if execution.is_canceled else 0,
        "canceled_by": execution.canceled_by,
        "cancellation_reason": execution.cancellation_reason,
        "paused": (json.dumps(paused_to_dict(execution.paused)) if execution.paused else None),
        "pipeline_config_id": execution.pipeline_config_id,
        "is_limit_concurrent": 1 if execution.is_limit_concurrent else 0,
        "max_concurrent_executions": execution.max_concurrent_executions,
        "keep_waiting_pipelines": 1 if execution.keep_waiting_pipelines else 0,
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


def row_to_execution(row: sqlite3.Row) -> Workflow:
    """Convert database row to Workflow."""
    trigger_data = json.loads(row["trigger"] or "{}")
    paused_data = json.loads(row["paused"]) if row["paused"] else None
    context_data = json.loads(row["context"] or "{}")

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
        context=context_data,
        start_time=row["start_time"],
        end_time=row["end_time"],
        start_time_expiry=row["start_time_expiry"],
        trigger=Trigger.from_dict(trigger_data),
        is_canceled=bool(row["is_canceled"]),
        canceled_by=row["canceled_by"],
        cancellation_reason=row["cancellation_reason"],
        paused=paused,
        pipeline_config_id=row["pipeline_config_id"],
        is_limit_concurrent=bool(row["is_limit_concurrent"]),
        max_concurrent_executions=row["max_concurrent_executions"] or 0,
        keep_waiting_pipelines=bool(row["keep_waiting_pipelines"]),
        origin=row["origin"] or "unknown",
    )


def row_to_stage(row: sqlite3.Row) -> StageExecution:
    """Convert database row to StageExecution."""
    from stabilize.models.multi_instance import MultiInstanceConfig
    from stabilize.models.stage import JoinType, SplitType

    context = json.loads(row["context"] or "{}")
    outputs = json.loads(row["outputs"] or "{}")
    requisite_ids = json.loads(row["requisite_stage_ref_ids"] or "[]")

    synthetic_owner = None
    if row["synthetic_stage_owner"]:
        synthetic_owner = SyntheticStageOwner(row["synthetic_stage_owner"])

    # Parse new control-flow fields with safe defaults for older schemas
    def _safe_get(key: str, default: Any = None) -> Any:
        try:
            return row[key]
        except (IndexError, KeyError):
            return default

    join_type_str = _safe_get("join_type", "AND")
    join_type = JoinType(join_type_str) if join_type_str else JoinType.AND

    split_type_str = _safe_get("split_type", "AND")
    split_type = SplitType(split_type_str) if split_type_str else SplitType.AND

    split_conditions_raw = _safe_get("split_conditions", "{}")
    split_conditions = json.loads(split_conditions_raw or "{}") if isinstance(split_conditions_raw, str) else {}

    mi_config_raw = _safe_get("mi_config")
    mi_config = None
    if mi_config_raw:
        mi_data = json.loads(mi_config_raw) if isinstance(mi_config_raw, str) else mi_config_raw
        if mi_data:
            mi_config = MultiInstanceConfig.from_dict(mi_data)

    return StageExecution(
        id=row["id"],
        ref_id=row["ref_id"],
        type=row["type"],
        name=row["name"] or "",
        status=WorkflowStatus[row["status"]],
        context=context,
        outputs=outputs,
        requisite_stage_ref_ids=set(requisite_ids),
        parent_stage_id=row["parent_stage_id"],
        synthetic_stage_owner=synthetic_owner,
        start_time=row["start_time"],
        end_time=row["end_time"],
        start_time_expiry=row["start_time_expiry"],
        scheduled_time=row["scheduled_time"],
        version=row["version"],
        join_type=join_type,
        join_threshold=_safe_get("join_threshold", 0) or 0,
        split_type=split_type,
        split_conditions=split_conditions,
        mi_config=mi_config,
        deferred_choice_group=_safe_get("deferred_choice_group"),
        milestone_ref_id=_safe_get("milestone_ref_id"),
        milestone_status=_safe_get("milestone_status"),
        mutex_key=_safe_get("mutex_key"),
        cancel_region=_safe_get("cancel_region"),
    )


def row_to_task(row: sqlite3.Row) -> TaskExecution:
    """Convert database row to TaskExecution."""
    exception_details = json.loads(row["task_exception_details"] or "{}")

    # Handle version column - may not exist in older schemas
    try:
        version = row["version"] or 0
    except (IndexError, KeyError):
        version = 0

    return TaskExecution(
        id=row["id"],
        name=row["name"],
        implementing_class=row["implementing_class"],
        status=WorkflowStatus[row["status"]],
        start_time=row["start_time"],
        end_time=row["end_time"],
        stage_start=bool(row["stage_start"]),
        stage_end=bool(row["stage_end"]),
        loop_start=bool(row["loop_start"]),
        loop_end=bool(row["loop_end"]),
        task_exception_details=exception_details,
        version=version,
    )
