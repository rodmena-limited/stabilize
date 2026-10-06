"""Workflow query criteria and exceptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from stabilize.models.status import WorkflowStatus


class WorkflowNotFoundError(Exception):
    """Raised when an execution cannot be found."""

    def __init__(self, execution_id: str):
        self.execution_id = execution_id
        super().__init__(f"Execution not found: {execution_id}")


@dataclass
class WorkflowCriteria:
    """Criteria for querying executions."""

    page_size: int | None = 20
    statuses: set[WorkflowStatus] | None = None
    start_time_before: int | None = None
    start_time_after: int | None = None


def time_window_sql(criteria: WorkflowCriteria | None, params: dict[str, Any], style: str) -> str:
    """SQL for the criteria's start_time window; NULL start_time is always inside it.

    style ":" renders :name placeholders (sqlite), "%" renders %(name)s (psycopg).
    """
    if criteria is None:
        return ""

    def ph(name: str) -> str:
        return f":{name}" if style == ":" else f"%({name})s"

    sql = ""
    if criteria.start_time_after is not None:
        sql += f" AND (start_time >= {ph('start_time_after')} OR start_time IS NULL)"
        params["start_time_after"] = criteria.start_time_after
    if criteria.start_time_before is not None:
        sql += f" AND (start_time <= {ph('start_time_before')} OR start_time IS NULL)"
        params["start_time_before"] = criteria.start_time_before
    return sql
