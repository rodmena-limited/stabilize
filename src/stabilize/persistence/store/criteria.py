"""Workflow query criteria and exceptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from stabilize.errors.workflow import WorkflowNotFoundError as WorkflowNotFoundError
from stabilize.models.status import WorkflowStatus


@dataclass
class WorkflowCriteria:
    """Criteria for querying executions."""

    page_size: int | None = 20
    statuses: set[WorkflowStatus] | None = None
    start_time_before: int | None = None
    start_time_after: int | None = None
    pipeline_config_version: str | None = None
    include_unstarted: bool = False
    after: str | None = None

    def validate(self) -> None:
        """Refuse a cursor this version cannot read."""
        if self.after is not None:
            parse_cursor(self.after)

    @staticmethod
    def cursor_after(workflow: Any) -> str:
        """The opaque cursor that continues a listing below *workflow*."""
        start = workflow.start_time
        return f"{CURSOR_VERSION}:{'' if start is None else int(start)}:{workflow.id}"


CURSOR_VERSION = "v1"

WORKFLOW_CRITERIA_FIELDS: frozenset[str] = frozenset(
    {
        "page_size",
        "statuses",
        "start_time_before",
        "start_time_after",
        "pipeline_config_version",
        "include_unstarted",
        "after",
    }
)


def parse_cursor(after: str) -> tuple[int | None, str]:
    """The (start_time, id) a cursor names; ValueError for a cursor of another version or shape."""
    version, _, rest = after.partition(":")
    start_text, sep, workflow_id = rest.partition(":")
    if version != CURSOR_VERSION or not sep or not workflow_id:
        raise ValueError(f"unreadable listing cursor {after!r}; expected {CURSOR_VERSION}:<start_time>:<id>")
    if start_text == "":
        return None, workflow_id
    try:
        return int(start_text), workflow_id
    except ValueError as exc:
        raise ValueError(f"unreadable listing cursor {after!r}: start_time is not an integer") from exc


def _placeholder(style: str, name: str) -> str:
    return f":{name}" if style == ":" else f"%({name})s"


def order_sql() -> str:
    """Newest start first, the unstarted last, ties broken by id; the same on both backends."""
    return " ORDER BY start_time DESC NULLS LAST, id DESC"


def cursor_sql(criteria: WorkflowCriteria | None, params: dict[str, Any], style: str) -> str:
    """SQL continuing the listing below the criteria's cursor, in the order order_sql() gives."""
    if criteria is None or criteria.after is None:
        return ""
    start, workflow_id = parse_cursor(criteria.after)
    params["cursor_id"] = workflow_id
    if start is None:
        return f" AND start_time IS NULL AND id < {_placeholder(style, 'cursor_id')}"
    params["cursor_start"] = start
    return (
        f" AND (start_time < {_placeholder(style, 'cursor_start')}"
        f" OR (start_time = {_placeholder(style, 'cursor_start')} AND id < {_placeholder(style, 'cursor_id')})"
        " OR start_time IS NULL)"
    )


def version_sql(criteria: WorkflowCriteria | None, params: dict[str, Any], style: str) -> str:
    """SQL for the criteria's definition version, when one is asked for."""
    if criteria is None or criteria.pipeline_config_version is None:
        return ""
    params["pipeline_config_version"] = criteria.pipeline_config_version
    placeholder = ":pipeline_config_version" if style == ":" else "%(pipeline_config_version)s"
    return f" AND pipeline_config_version = {placeholder}"


def time_window_sql(criteria: WorkflowCriteria | None, params: dict[str, Any], style: str) -> str:
    """SQL for the criteria's start_time window; a NULL start_time is outside it unless include_unstarted.

    style ":" renders :name placeholders (sqlite), "%" renders %(name)s (psycopg).
    """
    if criteria is None:
        return ""

    def ph(name: str) -> str:
        return f":{name}" if style == ":" else f"%({name})s"

    unstarted = " OR start_time IS NULL" if criteria.include_unstarted else ""
    sql = ""
    if criteria.start_time_after is not None:
        sql += f" AND (start_time >= {ph('start_time_after')}{unstarted})"
        params["start_time_after"] = criteria.start_time_after
    if criteria.start_time_before is not None:
        sql += f" AND (start_time <= {ph('start_time_before')}{unstarted})"
        params["start_time_before"] = criteria.start_time_before
    return sql
