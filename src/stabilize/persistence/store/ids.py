"""Identifier limits of the persisted schema."""

from __future__ import annotations

from typing import Any

MAX_ID_LENGTH = 26


def check_ids(execution: Any) -> None:
    """Refuse a workflow whose own, stage or task ids exceed the schema's 26-character columns."""
    too_long = [("workflow", execution.id)] if len(str(execution.id)) > MAX_ID_LENGTH else []
    for stage in execution.stages:
        if len(str(stage.id)) > MAX_ID_LENGTH:
            too_long.append(("stage", stage.id))
        too_long.extend(("task", t.id) for t in stage.tasks if len(str(t.id)) > MAX_ID_LENGTH)
    if too_long:
        kind, value = too_long[0]
        raise ValueError(
            f"{kind} id {value!r} is longer than {MAX_ID_LENGTH} characters ({len(too_long)} id(s) too long)"
        )
