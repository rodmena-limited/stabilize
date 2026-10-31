"""The JSON shape of a stage definition, as a sub-workflow configuration carries it in a stage context."""

from __future__ import annotations

from typing import Any

from stabilize.models.stage.enums import JoinType, SplitType
from stabilize.models.stage.stage import StageExecution
from stabilize.models.task import TaskExecution

_SCALAR_FIELDS = (
    "ref_id",
    "type",
    "name",
    "deferred_choice_group",
    "mutex_key",
    "cancel_region",
    "milestone_ref_id",
    "milestone_status",
    "start_time_expiry",
    "join_threshold",
    "cleanup_on_failure",
)
_TASK_FIELDS = ("name", "implementing_class", "stage_start", "stage_end", "loop_start", "loop_end")


def stage_to_definition(stage: StageExecution) -> dict[str, Any]:
    """A JSON-safe description of *stage* that ``stage_from_definition`` turns back into a fresh stage."""
    data: dict[str, Any] = {field: getattr(stage, field) for field in _SCALAR_FIELDS}
    data["requisite_stage_ref_ids"] = sorted(stage.requisite_stage_ref_ids)
    data["context"] = dict(stage.context)
    data["join_type"] = stage.join_type.name
    data["split_type"] = stage.split_type.name
    data["split_conditions"] = dict(stage.split_conditions)
    data["output_reducers"] = dict(stage.output_reducers)
    data["finalizer_names"] = list(stage.finalizer_names)
    data["tasks"] = [{field: getattr(task, field) for field in _TASK_FIELDS} for task in stage.tasks]
    return data


def stage_from_definition(data: StageExecution | dict[str, Any]) -> StageExecution:
    """A new stage built from a definition dict; a StageExecution passes through unchanged."""
    if isinstance(data, StageExecution):
        return data
    if not isinstance(data, dict):
        raise ValueError(f"a stage definition must be a mapping, not {type(data).__name__}")
    kwargs: dict[str, Any] = {field: data[field] for field in _SCALAR_FIELDS if field in data}
    kwargs["requisite_stage_ref_ids"] = set(data.get("requisite_stage_ref_ids") or ())
    kwargs["context"] = dict(data.get("context") or {})
    if "join_type" in data:
        kwargs["join_type"] = JoinType[str(data["join_type"])]
    if "split_type" in data:
        kwargs["split_type"] = SplitType[str(data["split_type"])]
    kwargs["split_conditions"] = dict(data.get("split_conditions") or {})
    kwargs["output_reducers"] = dict(data.get("output_reducers") or {})
    kwargs["finalizer_names"] = list(data.get("finalizer_names") or ())
    tasks = [
        TaskExecution.create(
            name=str(task.get("name", "")),
            implementing_class=str(task.get("implementing_class", "")),
            stage_start=bool(task.get("stage_start", False)),
            stage_end=bool(task.get("stage_end", False)),
        )
        for task in data.get("tasks") or ()
    ]
    for task, raw in zip(tasks, data.get("tasks") or (), strict=True):
        task.loop_start = bool(raw.get("loop_start", False))
        task.loop_end = bool(raw.get("loop_end", False))
    kwargs["tasks"] = tasks
    return StageExecution(**kwargs)
