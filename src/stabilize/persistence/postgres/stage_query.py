"""Stage queries on PostgreSQL: one statement per page, tasks and summary parents in two batches."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from stabilize.persistence.columns import select_list
from stabilize.persistence.postgres.converters import row_to_execution, row_to_stage
from stabilize.persistence.postgres.queries import load_tasks_for_stages
from stabilize.persistence.store.stage_criteria import SUPPORTED_CRITERIA, StageCriteria, stage_from, stage_where

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

_STAGE = select_list("stage_executions", "postgres", alias="s")
_PIPELINE = select_list("pipeline_executions", "postgres")


def stage_query_sql(store: Any, criteria: StageCriteria) -> tuple[str, dict[str, Any]]:
    """The page statement and its parameters, as the store runs it."""
    where, params = stage_where(criteria, store.tenant, "%")
    params["limit"] = int(criteria.page_size)
    sql = f"SELECT {_STAGE} {stage_from(criteria)} {where} ORDER BY s.id DESC LIMIT %(limit)s"
    return sql, params


def count_stages(store: Any, criteria: StageCriteria) -> int:
    where, params = stage_where(criteria, store.tenant, "%")
    with store._borrow() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) AS n {stage_from(criteria)} {where}", params)
            row = cur.fetchone()
        conn.rollback()
    return int(cast(dict[str, Any], row)["n"]) if row else 0


def retrieve_stages(store: Any, criteria: StageCriteria) -> list[StageExecution]:
    sql, params = stage_query_sql(store, criteria)
    with store._borrow() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = [cast(dict[str, Any], row) for row in cur.fetchall()]
            stages = [row_to_stage(row) for row in rows]
            owners = {stage.id: row["execution_id"] for stage, row in zip(stages, rows, strict=True)}
            parents: dict[str, Any] = {}
            if stages:
                load_tasks_for_stages(cur, stages, store.tenant)
                cur.execute(
                    f"SELECT {_PIPELINE} FROM pipeline_executions "
                    "WHERE id = ANY(%(ids)s) AND stabilize_tenant = %(tenant)s",
                    {"ids": sorted(set(owners.values())), "tenant": store.tenant},
                )
                parents = {r["id"]: row_to_execution(cast(dict[str, Any], r)) for r in cur.fetchall()}
        conn.rollback()
    for stage in stages:
        parent = parents.get(owners[stage.id])
        if parent is not None:
            stage.set_execution_strong(parent)
    return stages


class PostgresStageQueryMixin:
    tenant: str

    def supported_criteria(self) -> frozenset[str]:
        return SUPPORTED_CRITERIA

    def retrieve_stages(self, criteria: StageCriteria) -> list[StageExecution]:
        return retrieve_stages(self, criteria)

    def count_stages(self, criteria: StageCriteria) -> int:
        return count_stages(self, criteria)
