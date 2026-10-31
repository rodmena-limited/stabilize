"""StageCriteria: the stage query every backend answers, and the WHERE clause it compiles to."""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any

from stabilize.models.status import WorkflowStatus

MAX_PAGE_SIZE = 500

_SCALARS = (str, int, float, bool, type(None))


@dataclass
class StageCriteria:
    """What to match when listing stages across workflows.

    Time windows match only stages that have the time set. ``context_contains``
    matches top-level keys with scalar values. Pages run from the newest id
    downwards; ``after_id`` is the last id of the previous page.
    """

    statuses: set[WorkflowStatus] | None = None
    types: set[str] | None = None
    ref_ids: set[str] | None = None
    application: str | None = None
    execution_id: str | None = None
    start_time_after: int | None = None
    start_time_before: int | None = None
    end_time_after: int | None = None
    end_time_before: int | None = None
    scheduled_time_after: int | None = None
    scheduled_time_before: int | None = None
    context_contains: dict[str, Any] | None = field(default=None)
    page_size: int = 100
    after_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.page_size, int) or not 1 <= self.page_size <= MAX_PAGE_SIZE:
            raise ValueError(f"StageCriteria.page_size must be between 1 and {MAX_PAGE_SIZE}, got {self.page_size!r}")
        for key, value in (self.context_contains or {}).items():
            if not isinstance(key, str) or not key:
                raise ValueError(f"StageCriteria.context_contains needs string keys, got {key!r}")
            if not isinstance(value, _SCALARS):
                raise ValueError(
                    f"StageCriteria.context_contains supports scalar values only; key {key!r} holds a nested value"
                )

    @property
    def needs_workflow_join(self) -> bool:
        return self.application is not None


SUPPORTED_CRITERIA: frozenset[str] = frozenset(f.name for f in fields(StageCriteria))

_WINDOWS: tuple[tuple[str, str, str], ...] = (
    ("start_time_after", "start_time", ">="),
    ("start_time_before", "start_time", "<="),
    ("end_time_after", "end_time", ">="),
    ("end_time_before", "end_time", "<="),
    ("scheduled_time_after", "scheduled_time", ">="),
    ("scheduled_time_before", "scheduled_time", "<="),
)


def _placeholder(style: str, name: str) -> str:
    return f":{name}" if style == ":" else f"%({name})s"


def _membership(style: str, column: str, name: str, values: list[Any], params: dict[str, Any]) -> str:
    if style == "%":
        params[name] = values
        return f" AND {column} = ANY(%({name})s)"
    names = []
    for index, value in enumerate(values):
        params[f"{name}_{index}"] = value
        names.append(f":{name}_{index}")
    return f" AND {column} IN ({', '.join(names)})"


def _json_path(key: str) -> str:
    return '$."' + key.replace("\\", "\\\\").replace('"', '\\"') + '"'


def stage_where(criteria: StageCriteria, tenant: str, style: str, alias: str = "s") -> tuple[str, dict[str, Any]]:
    """The WHERE clause (starting with ``WHERE``) and its parameters for *criteria* on *tenant*.

    style "%" renders psycopg placeholders and PostgreSQL operators, ":" renders SQLite's.
    """
    import json

    params: dict[str, Any] = {"tenant": tenant}
    sql = f"WHERE {alias}.stabilize_tenant = {_placeholder(style, 'tenant')}"
    if criteria.statuses:
        sql += _membership(style, f"{alias}.status", "statuses", sorted(s.name for s in criteria.statuses), params)
    if criteria.types:
        sql += _membership(style, f"{alias}.type", "types", sorted(criteria.types), params)
    if criteria.ref_ids:
        sql += _membership(style, f"{alias}.ref_id", "ref_ids", sorted(criteria.ref_ids), params)
    if criteria.application is not None:
        sql += f" AND p.application = {_placeholder(style, 'application')}"
        params["application"] = criteria.application
    if criteria.execution_id is not None:
        sql += f" AND {alias}.execution_id = {_placeholder(style, 'execution_id')}"
        params["execution_id"] = criteria.execution_id
    for name, column, op in _WINDOWS:
        bound = getattr(criteria, name)
        if bound is not None:
            sql += f" AND {alias}.{column} IS NOT NULL AND {alias}.{column} {op} {_placeholder(style, name)}"
            params[name] = int(bound)
    if criteria.context_contains:
        if style == "%":
            sql += f" AND {alias}.context @> %(context_contains)s::jsonb"
            params["context_contains"] = json.dumps(criteria.context_contains)
        else:
            for index, (key, value) in enumerate(sorted(criteria.context_contains.items())):
                params[f"ctx_path_{index}"] = _json_path(key)
                if value is None:
                    sql += f" AND json_type({alias}.context, :ctx_path_{index}) = 'null'"
                else:
                    params[f"ctx_val_{index}"] = value
                    sql += f" AND json_extract({alias}.context, :ctx_path_{index}) = :ctx_val_{index}"
    if criteria.after_id is not None:
        sql += f" AND {alias}.id < {_placeholder(style, 'after_id')}"
        params["after_id"] = criteria.after_id
    return sql, params


def stage_from(criteria: StageCriteria, alias: str = "s") -> str:
    """The FROM clause, joining the workflow row only when a workflow column is filtered on."""
    sql = f"FROM stage_executions {alias}"
    if criteria.needs_workflow_join:
        sql += (
            f" JOIN pipeline_executions p ON p.id = {alias}.execution_id"
            f" AND p.stabilize_tenant = {alias}.stabilize_tenant"
        )
    return sql
