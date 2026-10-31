"""Stage queries on SQLite: one statement per page, tasks and summary parents in two batches."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from stabilize.persistence.columns import select_list
from stabilize.persistence.sqlite.converters import row_to_execution, row_to_stage, row_to_task
from stabilize.persistence.store.stage_criteria import SUPPORTED_CRITERIA, StageCriteria, stage_from, stage_where

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution

_STAGE = select_list("stage_executions", "sqlite", alias="s")
_PIPELINE = select_list("pipeline_executions", "sqlite")
_TASK = select_list("task_executions", "sqlite")


def _in_list(name: str, values: list[str]) -> tuple[str, dict[str, str]]:
    names = [f":{name}_{i}" for i in range(len(values))]
    return ", ".join(names), {f"{name}_{i}": v for i, v in enumerate(values)}


class SqliteStageQueryMixin:
    tenant: str

    if TYPE_CHECKING:

        def _get_connection(self) -> sqlite3.Connection: ...

    def supported_criteria(self) -> frozenset[str]:
        return SUPPORTED_CRITERIA

    def count_stages(self, criteria: StageCriteria) -> int:
        where, params = stage_where(criteria, self.tenant, ":")
        row = self._get_connection().execute(f"SELECT count(*) {stage_from(criteria)} {where}", params).fetchone()
        return int(row[0]) if row else 0

    def retrieve_stages(self, criteria: StageCriteria) -> list[StageExecution]:
        conn = self._get_connection()
        where, params = stage_where(criteria, self.tenant, ":")
        params["limit"] = int(criteria.page_size)
        rows = conn.execute(
            f"SELECT {_STAGE} {stage_from(criteria)} {where} ORDER BY s.id DESC LIMIT :limit", params
        ).fetchall()
        stages = [row_to_stage(row) for row in rows]
        if not stages:
            return []
        owners = {stage.id: row["execution_id"] for stage, row in zip(stages, rows, strict=True)}
        by_id = {s.id: s for s in stages}
        placeholders, task_params = _in_list("stage", list(by_id))
        task_params["tenant"] = self.tenant
        for task_row in conn.execute(
            f"SELECT {_TASK} FROM task_executions WHERE stage_id IN ({placeholders}) AND stabilize_tenant = :tenant "
            "ORDER BY id ASC",
            task_params,
        ).fetchall():
            task = row_to_task(task_row)
            owner = by_id[task_row["stage_id"]]
            task.stage = owner
            owner.tasks.append(task)
        placeholders, parent_params = _in_list("wf", sorted(set(owners.values())))
        parent_params["tenant"] = self.tenant
        parents = {
            row["id"]: row_to_execution(row)
            for row in conn.execute(
                f"SELECT {_PIPELINE} FROM pipeline_executions WHERE id IN ({placeholders}) "
                "AND stabilize_tenant = :tenant",
                parent_params,
            ).fetchall()
        }
        for stage in stages:
            parent = parents.get(owners[stage.id])
            if parent is not None:
                stage.set_execution_strong(parent)
        return stages
