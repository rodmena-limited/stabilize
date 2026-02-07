"""Complex query operations for SQLite persistence."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

from stabilize.persistence.sqlite.converters import row_to_stage, row_to_task
from stabilize.persistence.store import WorkflowCriteria

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow


def load_tasks_for_stages(
    conn: sqlite3.Connection,
    stages: list[StageExecution],
) -> None:
    """Load tasks for a list of stages."""
    if not stages:
        return

    stage_map = {s.id: s for s in stages}
    stage_ids = list(stage_map.keys())

    # Build query for stage_ids
    placeholders = ", ".join(f":sid_{i}" for i in range(len(stage_ids)))
    params = {}
    for i, sid in enumerate(stage_ids):
        params[f"sid_{i}"] = sid

    result = conn.execute(
        f"""
        SELECT * FROM task_executions
        WHERE stage_id IN ({placeholders})
        ORDER BY id ASC
        """,
        params,
    )

    for row in result.fetchall():
        task = row_to_task(row)
        stage_ref = stage_map.get(row["stage_id"])
        if stage_ref:
            task.stage = stage_ref
            stage_ref.tasks.append(task)


def set_execution_reference(
    conn: sqlite3.Connection,
    stages: list[StageExecution],
    execution_id: str,
) -> None:
    """Set the _execution reference for stages by loading execution summary.

    Also loads all stages for the execution to populate execution.stages,
    which is required for methods like synthetic_stages() and after_stages().
    """
    from stabilize.persistence.sqlite.converters import row_to_execution

    if not stages:
        return

    result = conn.execute(
        "SELECT * FROM pipeline_executions WHERE id = :id",
        {"id": execution_id},
    )
    exec_row = result.fetchone()
    if exec_row:
        execution = row_to_execution(exec_row)

        # Load all stages for this execution to populate execution.stages
        # This is required for synthetic_stages() and after_stages() to work
        all_stages_result = conn.execute(
            "SELECT * FROM stage_executions WHERE execution_id = :id",
            {"id": execution_id},
        )
        all_stages = [row_to_stage(row) for row in all_stages_result.fetchall()]
        execution.stages = all_stages

        # Set execution reference for all loaded stages
        for s in all_stages:
            s.execution = execution

        # Also set for the originally requested stages (they may be the same objects)
        for stage in stages:
            stage.execution = execution


def get_upstream_stages(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
) -> list[StageExecution]:
    """Get upstream stages with tasks loaded."""
    # First find the requisite ref ids of the target stage
    result = conn.execute(
        """
        SELECT requisite_stage_ref_ids FROM stage_executions
        WHERE execution_id = :execution_id AND ref_id = :ref_id
        """,
        {"execution_id": execution_id, "ref_id": stage_ref_id},
    )
    row = result.fetchone()
    if not row or not row["requisite_stage_ref_ids"]:
        return []

    requisites = json.loads(row["requisite_stage_ref_ids"] or "[]")
    if not requisites:
        return []

    # Build query for ref_ids
    placeholders = ", ".join(f":ref_{i}" for i in range(len(requisites)))
    params: dict[str, Any] = {"execution_id": execution_id}
    for i, ref in enumerate(requisites):
        params[f"ref_{i}"] = ref

    result = conn.execute(
        f"""
        SELECT * FROM stage_executions
        WHERE execution_id = :execution_id
        AND ref_id IN ({placeholders})
        """,
        params,
    )

    stages = []
    for stage_row in result.fetchall():
        stage = row_to_stage(stage_row)
        stages.append(stage)

    # Load tasks for all stages
    load_tasks_for_stages(conn, stages)

    # Set execution reference
    set_execution_reference(conn, stages, execution_id)

    return stages


def get_downstream_stages(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
) -> list[StageExecution]:
    """Get downstream stages with tasks loaded."""
    # Use json_each for JSON array membership test (available in Python 3.11+ sqlite)
    query = """
        SELECT stage_executions.* FROM stage_executions, json_each(stage_executions.requisite_stage_ref_ids)
        WHERE execution_id = :execution_id
        AND json_each.value = :ref_id
    """
    result = conn.execute(query, {"execution_id": execution_id, "ref_id": stage_ref_id})

    stages = []
    for stage_row in result.fetchall():
        stage = row_to_stage(stage_row)
        stages.append(stage)

    # Load tasks for all stages
    load_tasks_for_stages(conn, stages)

    # Set execution reference
    set_execution_reference(conn, stages, execution_id)

    return stages


def get_synthetic_stages(
    conn: sqlite3.Connection,
    execution_id: str,
    parent_stage_id: str,
) -> list[StageExecution]:
    """Get synthetic stages with tasks loaded."""
    result = conn.execute(
        """
        SELECT * FROM stage_executions
        WHERE execution_id = :execution_id
        AND parent_stage_id = :parent_id
        """,
        {"execution_id": execution_id, "parent_id": parent_stage_id},
    )

    stages = []
    for stage_row in result.fetchall():
        stage = row_to_stage(stage_row)
        stages.append(stage)

    # Load tasks for all stages
    load_tasks_for_stages(conn, stages)

    # Set execution reference
    set_execution_reference(conn, stages, execution_id)

    return stages


def get_merged_ancestor_outputs(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
) -> dict[str, Any]:
    """Get merged outputs from all ancestor stages."""
    # Fetch lightweight graph (ref_id, requisites, outputs)
    result = conn.execute(
        """
        SELECT ref_id, requisite_stage_ref_ids, outputs
        FROM stage_executions
        WHERE execution_id = :execution_id
        """,
        {"execution_id": execution_id},
    )
    rows = result.fetchall()

    # Build graph in memory
    nodes = {}
    for row in rows:
        req_ids = json.loads(row["requisite_stage_ref_ids"] or "[]")
        outputs = json.loads(row["outputs"] or "{}")
        nodes[row["ref_id"]] = {
            "requisites": set(req_ids),
            "outputs": outputs,
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

    # Calculate in-degrees within the subgraph of ancestors
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
    merged_result: dict[str, Any] = {}
    for aid in sorted_ancestors:
        outputs = nodes[aid]["outputs"]
        for key, value in outputs.items():
            if (
                key in merged_result
                and isinstance(merged_result[key], list)
                and isinstance(value, list)
            ):
                # Concatenate lists
                existing = merged_result[key]
                for item in value:
                    if item not in existing:
                        existing.append(item)
            else:
                merged_result[key] = value

    return merged_result


def retrieve_by_pipeline_config_id(
    conn: sqlite3.Connection,
    pipeline_config_id: str,
    criteria: WorkflowCriteria | None,
    retrieve_fn: Callable[[str], Workflow],
) -> Iterator[Workflow]:
    """Retrieve executions by pipeline config ID."""
    query = """
        SELECT id FROM pipeline_executions
        WHERE pipeline_config_id = :config_id
    """
    params: dict[str, Any] = {"config_id": pipeline_config_id}

    if criteria and criteria.statuses:
        status_names = [s.name for s in criteria.statuses]
        placeholders = ", ".join(f":status_{i}" for i in range(len(status_names)))
        query += f" AND status IN ({placeholders})"
        for i, name in enumerate(status_names):
            params[f"status_{i}"] = name

    query += " ORDER BY start_time DESC"

    if criteria and criteria.page_size:
        query += f" LIMIT {criteria.page_size}"

    result = conn.execute(query, params)
    for row in result.fetchall():
        yield retrieve_fn(row[0])


def retrieve_by_application(
    conn: sqlite3.Connection,
    application: str,
    criteria: WorkflowCriteria | None,
    retrieve_fn: Callable[[str], Workflow],
) -> Iterator[Workflow]:
    """Retrieve executions by application."""
    query = """
        SELECT id FROM pipeline_executions
        WHERE application = :application
    """
    params: dict[str, Any] = {"application": application}

    if criteria and criteria.statuses:
        status_names = [s.name for s in criteria.statuses]
        placeholders = ", ".join(f":status_{i}" for i in range(len(status_names)))
        query += f" AND status IN ({placeholders})"
        for i, name in enumerate(status_names):
            params[f"status_{i}"] = name

    query += " ORDER BY start_time DESC"

    if criteria and criteria.page_size:
        query += f" LIMIT {criteria.page_size}"

    result = conn.execute(query, params)
    for row in result.fetchall():
        yield retrieve_fn(row[0])
