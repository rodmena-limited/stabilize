"""Complex query operations for PostgreSQL persistence."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, cast

from stabilize.persistence.postgres.converters import (
    row_to_execution,
    row_to_stage,
    row_to_task,
)
from stabilize.persistence.store import WorkflowCriteria

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow


def load_tasks_for_stages(cur: Any, stages: list[StageExecution]) -> None:
    """Load tasks for a list of stages in a single query."""
    if not stages:
        return

    stage_map = {s.id: s for s in stages}
    stage_ids = list(stage_map.keys())

    # ORDER BY id ensures consistent task sequencing (ULID encodes creation time)
    cur.execute(
        """
        SELECT * FROM task_executions
        WHERE stage_id = ANY(%(stage_ids)s)
        ORDER BY id ASC
        """,
        {"stage_ids": stage_ids},
    )

    for row in cur.fetchall():
        task_row = cast(dict[str, Any], row)
        task = row_to_task(task_row)
        stage_ref = stage_map.get(task_row["stage_id"])
        if stage_ref:
            task.stage = stage_ref
            stage_ref.tasks.append(task)


def set_execution_reference(cur: Any, stages: list[StageExecution], execution_id: str) -> None:
    """Set the _execution reference for stages by loading execution summary."""
    if not stages:
        return

    cur.execute(
        "SELECT * FROM pipeline_executions WHERE id = %(id)s",
        {"id": execution_id},
    )
    exec_row = cur.fetchone()
    if exec_row:
        execution = row_to_execution(cast(dict[str, Any], exec_row))

        # Load all stages for this execution
        cur.execute(
            "SELECT * FROM stage_executions WHERE execution_id = %(id)s",
            {"id": execution_id},
        )
        all_stages = [row_to_stage(cast(dict[str, Any], row)) for row in cur.fetchall()]
        execution.stages = all_stages

        # Set execution reference for all loaded stages
        for s in all_stages:
            s.execution = execution

        # Also set for the originally requested stages
        for stage in stages:
            stage.execution = execution


def get_upstream_stages(pool: Any, execution_id: str, stage_ref_id: str) -> list[StageExecution]:
    """Get upstream stages with tasks loaded."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # First find the requisite ref ids of the target stage
            cur.execute(
                """
                SELECT requisite_stage_ref_ids FROM stage_executions
                WHERE execution_id = %(execution_id)s AND ref_id = %(ref_id)s
                """,
                {"execution_id": execution_id, "ref_id": stage_ref_id},
            )
            row = cur.fetchone()
            if not row or not row["requisite_stage_ref_ids"]:
                return []

            requisites = list(row["requisite_stage_ref_ids"])

            # Now fetch those stages
            cur.execute(
                """
                SELECT * FROM stage_executions
                WHERE execution_id = %(execution_id)s
                AND ref_id = ANY(%(requisites)s)
                """,
                {"execution_id": execution_id, "requisites": requisites},
            )

            stages = []
            for stage_row in cur.fetchall():
                stage = row_to_stage(cast(dict[str, Any], stage_row))
                stages.append(stage)

            # Load tasks for all stages
            load_tasks_for_stages(cur, stages)

            # Set execution reference
            set_execution_reference(cur, stages, execution_id)

            return stages


def get_downstream_stages(pool: Any, execution_id: str, stage_ref_id: str) -> list[StageExecution]:
    """Get downstream stages with tasks loaded."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Find stages that have stage_ref_id in their requisites
            cur.execute(
                """
                SELECT * FROM stage_executions
                WHERE execution_id = %(execution_id)s
                AND %(ref_id)s = ANY(requisite_stage_ref_ids)
                """,
                {"execution_id": execution_id, "ref_id": stage_ref_id},
            )

            stages = []
            for stage_row in cur.fetchall():
                stage = row_to_stage(cast(dict[str, Any], stage_row))
                stages.append(stage)

            # Load tasks for all stages
            load_tasks_for_stages(cur, stages)

            # Set execution reference
            set_execution_reference(cur, stages, execution_id)

            return stages


def get_synthetic_stages(pool: Any, execution_id: str, parent_stage_id: str) -> list[StageExecution]:
    """Get synthetic stages with tasks loaded."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM stage_executions
                WHERE execution_id = %(execution_id)s
                AND parent_stage_id = %(parent_id)s
                """,
                {"execution_id": execution_id, "parent_id": parent_stage_id},
            )

            stages = []
            for stage_row in cur.fetchall():
                stage = row_to_stage(cast(dict[str, Any], stage_row))
                stages.append(stage)

            # Load tasks for all stages
            load_tasks_for_stages(cur, stages)

            # Set execution reference
            set_execution_reference(cur, stages, execution_id)

            return stages


def get_merged_ancestor_outputs(pool: Any, execution_id: str, stage_ref_id: str) -> dict[str, Any]:
    """Get merged outputs from all ancestor stages."""
    # Fetch lightweight graph (ref_id, requisites, outputs)
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ref_id, requisite_stage_ref_ids, outputs
                FROM stage_executions
                WHERE execution_id = %(execution_id)s
                """,
                {"execution_id": execution_id},
            )
            rows = cur.fetchall()

    # Build graph in memory
    nodes: dict[str, dict[str, Any]] = {}
    for row in rows:
        ref_id = row["ref_id"]
        requisites = row["requisite_stage_ref_ids"]
        outputs_raw = row["outputs"]
        nodes[ref_id] = {
            "requisites": set(requisites or []),
            "outputs": (outputs_raw if isinstance(outputs_raw, dict) else json.loads(outputs_raw or "{}")),
        }

    if stage_ref_id not in nodes:
        return {}

    # Find ancestors via BFS
    ancestors = set()
    queue = [stage_ref_id]
    visited = {stage_ref_id}

    while queue:
        current = queue.pop(0)
        node = nodes.get(current)
        if not node:
            continue

        for req in node["requisites"]:
            if req not in visited:
                visited.add(req)
                ancestors.add(req)
                queue.append(req)

    # Topological sort of ancestors
    sorted_ancestors = []
    in_degree = {aid: 0 for aid in ancestors}
    graph: dict[str, list[str]] = {aid: [] for aid in ancestors}

    for aid in ancestors:
        for req in nodes[aid]["requisites"]:
            if req in ancestors:
                graph[req].append(aid)
                in_degree[aid] += 1

    # Kahn's algorithm
    queue = [aid for aid in ancestors if in_degree[aid] == 0]
    while queue:
        u = queue.pop(0)
        sorted_ancestors.append(u)
        for v in graph[u]:
            in_degree[v] -= 1
            if in_degree[v] == 0:
                queue.append(v)

    # Merge outputs
    result: dict[str, Any] = {}
    for aid in sorted_ancestors:
        outputs: dict[str, Any] = nodes[aid]["outputs"]
        for key, value in outputs.items():
            if key in result and isinstance(result[key], list) and isinstance(value, list):
                # Concatenate lists
                existing = result[key]
                for item in value:
                    if item not in existing:
                        existing.append(item)
            else:
                result[key] = value

    return result


def retrieve_by_pipeline_config_id(
    pool: Any,
    pipeline_config_id: str,
    criteria: WorkflowCriteria | None,
    retrieve_fn: Callable[[str], Workflow],
) -> Iterator[Workflow]:
    """Retrieve executions by pipeline config ID."""
    query = """
        SELECT id FROM pipeline_executions
        WHERE pipeline_config_id = %(config_id)s
    """
    params: dict[str, Any] = {"config_id": pipeline_config_id}

    if criteria:
        if criteria.statuses:
            status_names = [s.name for s in criteria.statuses]
            query += " AND status = ANY(%(statuses)s)"
            params["statuses"] = status_names

    query += " ORDER BY start_time DESC"

    if criteria and criteria.page_size:
        query += f" LIMIT {criteria.page_size}"

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            for row in cur.fetchall():
                yield retrieve_fn(cast(dict[str, Any], row)["id"])


def retrieve_by_application(
    pool: Any,
    application: str,
    criteria: WorkflowCriteria | None,
    retrieve_fn: Callable[[str], Workflow],
) -> Iterator[Workflow]:
    """Retrieve executions by application."""
    query = """
        SELECT id FROM pipeline_executions
        WHERE application = %(application)s
    """
    params: dict[str, Any] = {"application": application}

    if criteria:
        if criteria.statuses:
            status_names = [s.name for s in criteria.statuses]
            query += " AND status = ANY(%(statuses)s)"
            params["statuses"] = status_names

    query += " ORDER BY start_time DESC"

    if criteria and criteria.page_size:
        query += f" LIMIT {criteria.page_size}"

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            for row in cur.fetchall():
                yield retrieve_fn(cast(dict[str, Any], row)["id"])
