"""
PostgreSQL execution repository.

Production-grade persistence using native psycopg3 with connection pooling.
Uses singleton ConnectionManager for efficient connection pool sharing.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any, cast

from stabilize.models.stage import StageExecution, SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import (
    PausedDetails,
    Trigger,
    Workflow,
    WorkflowType,
)
from stabilize.persistence.store import (
    WorkflowCriteria,
    WorkflowNotFoundError,
    WorkflowStore,
)


class PostgresWorkflowStore(WorkflowStore):
    """
    PostgreSQL implementation of WorkflowStore.

    Uses native psycopg3 with connection pooling for database operations.
    Supports concurrent access and provides efficient queries for pipeline
    execution tracking.

    Connection pools are managed by singleton ConnectionManager for
    efficient resource sharing across all repository instances.
    """

    def __init__(self, connection_string: str) -> None:
        """
        Initialize the repository.

        Args:
            connection_string: PostgreSQL connection string

        Note:
            Tables must be created using migrations (migretti).
            Run: stabilize migrate --database <connection_string>
        """
        from stabilize.persistence.connection import get_connection_manager

        self.connection_string = connection_string
        self._manager = get_connection_manager()
        self._pool = self._manager.get_postgres_pool(connection_string)

    def close(self) -> None:
        """Close the connection pool via connection manager."""
        self._manager.close_postgres_pool(self.connection_string)

    def store(self, execution: Workflow) -> None:
        """Store a complete execution."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                # Insert execution
                cur.execute(
                    """
                    INSERT INTO pipeline_executions (
                        id, type, application, name, status, start_time, end_time,
                        start_time_expiry, trigger, is_canceled, canceled_by,
                        cancellation_reason, paused, pipeline_config_id,
                        is_limit_concurrent, max_concurrent_executions,
                        keep_waiting_pipelines, origin
                    ) VALUES (
                        %(id)s, %(type)s, %(application)s, %(name)s, %(status)s,
                        %(start_time)s, %(end_time)s, %(start_time_expiry)s,
                        %(trigger)s::jsonb, %(is_canceled)s, %(canceled_by)s,
                        %(cancellation_reason)s, %(paused)s::jsonb, %(pipeline_config_id)s,
                        %(is_limit_concurrent)s, %(max_concurrent_executions)s,
                        %(keep_waiting_pipelines)s, %(origin)s
                    )
                    """,
                    self._execution_to_dict(execution),
                )

                # Insert stages
                for stage in execution.stages:
                    self._insert_stage(cur, stage, execution.id)

            conn.commit()

    def retrieve(self, execution_id: str) -> Workflow:
        """Retrieve an execution by ID."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                # Get execution
                cur.execute(
                    "SELECT * FROM pipeline_executions WHERE id = %(id)s",
                    {"id": execution_id},
                )
                row = cur.fetchone()
                if not row:
                    raise WorkflowNotFoundError(execution_id)

                execution = self._row_to_execution(cast(dict[str, Any], row))

                # Get stages
                cur.execute(
                    """
                    SELECT * FROM stage_executions
                    WHERE execution_id = %(execution_id)s
                    """,
                    {"execution_id": execution_id},
                )
                stages = []
                for stage_row in cur.fetchall():
                    stage = self._row_to_stage(cast(dict[str, Any], stage_row))
                    stage._execution = execution

                    # Get tasks for stage
                    cur.execute(
                        """
                        SELECT * FROM task_executions
                        WHERE stage_id = %(stage_id)s
                        """,
                        {"stage_id": stage.id},
                    )
                    for task_row in cur.fetchall():
                        task = self._row_to_task(cast(dict[str, Any], task_row))
                        task._stage = stage
                        stage.tasks.append(task)

                    stages.append(stage)

                execution.stages = stages
                return execution

    def retrieve_execution_summary(self, execution_id: str) -> Workflow:
        """Retrieve execution metadata without stages."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM pipeline_executions WHERE id = %(id)s",
                    {"id": execution_id},
                )
                row = cur.fetchone()
                if not row:
                    raise WorkflowNotFoundError(execution_id)

                return self._row_to_execution(cast(dict[str, Any], row))

    def update_status(self, execution: Workflow) -> None:
        """Update execution status."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline_executions SET
                        status = %(status)s,
                        start_time = %(start_time)s,
                        end_time = %(end_time)s,
                        is_canceled = %(is_canceled)s,
                        canceled_by = %(canceled_by)s,
                        cancellation_reason = %(cancellation_reason)s,
                        paused = %(paused)s::jsonb
                    WHERE id = %(id)s
                    """,
                    {
                        "id": execution.id,
                        "status": execution.status.name,
                        "start_time": execution.start_time,
                        "end_time": execution.end_time,
                        "is_canceled": execution.is_canceled,
                        "canceled_by": execution.canceled_by,
                        "cancellation_reason": execution.cancellation_reason,
                        "paused": (json.dumps(self._paused_to_dict(execution.paused)) if execution.paused else None),
                    },
                )
            conn.commit()

    def delete(self, execution_id: str) -> None:
        """Delete an execution."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pipeline_executions WHERE id = %(id)s",
                    {"id": execution_id},
                )
            conn.commit()

    def store_stage(self, stage: StageExecution) -> None:
        """Store or update a stage."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                # Check if stage exists
                cur.execute(
                    "SELECT id FROM stage_executions WHERE id = %(id)s",
                    {"id": stage.id},
                )
                exists = cur.fetchone() is not None

                if exists:
                    # Update
                    cur.execute(
                        """
                        UPDATE stage_executions SET
                            status = %(status)s,
                            context = %(context)s::jsonb,
                            outputs = %(outputs)s::jsonb,
                            start_time = %(start_time)s,
                            end_time = %(end_time)s
                        WHERE id = %(id)s
                        """,
                        {
                            "id": stage.id,
                            "status": stage.status.name,
                            "context": json.dumps(stage.context),
                            "outputs": json.dumps(stage.outputs),
                            "start_time": stage.start_time,
                            "end_time": stage.end_time,
                        },
                    )

                    # Update tasks
                    for task in stage.tasks:
                        self._upsert_task(cur, task, stage.id)
                else:
                    self._insert_stage(cur, stage, stage.execution.id)

            conn.commit()

    def add_stage(self, stage: StageExecution) -> None:
        """Add a new stage."""
        self.store_stage(stage)

    def remove_stage(
        self,
        execution: Workflow,
        stage_id: str,
    ) -> None:
        """Remove a stage."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM stage_executions WHERE id = %(id)s",
                    {"id": stage_id},
                )
            conn.commit()

    def retrieve_stage(self, stage_id: str) -> StageExecution:
        """Retrieve a single stage by ID."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                # Get stage
                cur.execute(
                    "SELECT * FROM stage_executions WHERE id = %(id)s",
                    {"id": stage_id},
                )
                stage_row = cur.fetchone()
                if not stage_row:
                    raise ValueError(f"Stage {stage_id} not found")

                stage = self._row_to_stage(cast(dict[str, Any], stage_row))

                # Get execution summary for context
                # We do this in a separate transaction effectively, but here we reuse connection
                cur.execute(
                    "SELECT * FROM pipeline_executions WHERE id = %(id)s",
                    {"id": stage_row["execution_id"]},  # type: ignore[call-overload]
                )
                exec_row = cur.fetchone()
                if exec_row:
                    execution = self._row_to_execution(cast(dict[str, Any], exec_row))
                    stage._execution = execution
                    # Add stage to execution's stage list so it can be found
                    execution.stages = [stage]

                # Get tasks
                cur.execute(
                    """
                    SELECT * FROM task_executions
                    WHERE stage_id = %(stage_id)s
                    """,
                    {"stage_id": stage.id},
                )
                for task_row in cur.fetchall():
                    task = self._row_to_task(cast(dict[str, Any], task_row))
                    task._stage = stage
                    stage.tasks.append(task)

                return stage

    def get_upstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get upstream stages."""
        with self._pool.connection() as conn:
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
                if not row or not row["requisite_stage_ref_ids"]:  # type: ignore[call-overload]
                    return []

                requisites = list(row["requisite_stage_ref_ids"])  # type: ignore[call-overload]

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
                    stage = self._row_to_stage(cast(dict[str, Any], stage_row))
                    stages.append(stage)

                return stages

    def get_downstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get downstream stages."""
        with self._pool.connection() as conn:
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
                    stage = self._row_to_stage(cast(dict[str, Any], stage_row))
                    # We don't populate tasks or full execution here for performance
                    # But we need execution ID
                    stages.append(stage)

                return stages

    def get_synthetic_stages(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[StageExecution]:
        """Get synthetic stages."""
        with self._pool.connection() as conn:
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
                    stage = self._row_to_stage(cast(dict[str, Any], stage_row))
                    stages.append(stage)

                return stages

    def get_merged_ancestor_outputs(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> dict[str, Any]:
        """Get merged outputs from all ancestor stages."""
        # Fetch lightweight graph (ref_id, requisites, outputs)
        with self._pool.connection() as conn:
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
            ref_id = row["ref_id"]  # type: ignore[call-overload]
            requisites = row["requisite_stage_ref_ids"]  # type: ignore[call-overload]
            outputs_raw = row["outputs"]  # type: ignore[call-overload]
            nodes[ref_id] = {
                "requisites": set(requisites or []),
                "outputs": outputs_raw if isinstance(outputs_raw, dict) else json.loads(outputs_raw or "{}"),
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
        # We only care about sorting 'ancestors' subset
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
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
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

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                for row in cur.fetchall():
                    yield self.retrieve(cast(dict[str, Any], row)["id"])

    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
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

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                for row in cur.fetchall():
                    yield self.retrieve(cast(dict[str, Any], row)["id"])

    def pause(self, execution_id: str, paused_by: str) -> None:
        """Pause an execution."""
        paused = PausedDetails(
            paused_by=paused_by,
            pause_time=int(time.time() * 1000),
        )

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline_executions SET
                        status = %(status)s,
                        paused = %(paused)s::jsonb
                    WHERE id = %(id)s
                    """,
                    {
                        "id": execution_id,
                        "status": WorkflowStatus.PAUSED.name,
                        "paused": json.dumps(self._paused_to_dict(paused)),
                    },
                )
            conn.commit()

    def resume(self, execution_id: str) -> None:
        """Resume a paused execution."""
        # First get current paused details
        execution = self.retrieve(execution_id)
        if execution.paused and execution.paused.pause_time:
            current_time = int(time.time() * 1000)
            execution.paused.resume_time = current_time
            execution.paused.paused_ms = current_time - execution.paused.pause_time

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline_executions SET
                        status = %(status)s,
                        paused = %(paused)s::jsonb
                    WHERE id = %(id)s
                    """,
                    {
                        "id": execution_id,
                        "status": WorkflowStatus.RUNNING.name,
                        "paused": (json.dumps(self._paused_to_dict(execution.paused)) if execution.paused else None),
                    },
                )
            conn.commit()

    def cancel(
        self,
        execution_id: str,
        canceled_by: str,
        reason: str,
    ) -> None:
        """Cancel an execution."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pipeline_executions SET
                        is_canceled = TRUE,
                        canceled_by = %(canceled_by)s,
                        cancellation_reason = %(reason)s
                    WHERE id = %(id)s
                    """,
                    {
                        "id": execution_id,
                        "canceled_by": canceled_by,
                        "reason": reason,
                    },
                )
            conn.commit()

    # ========== Message Deduplication ==========

    def is_message_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM processed_messages WHERE message_id = %(message_id)s",
                    {"message_id": message_id},
                )
                return cur.fetchone() is not None

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO processed_messages (
                        message_id, processed_at, handler_type, execution_id
                    ) VALUES (
                        %(message_id)s, NOW(), %(handler_type)s, %(execution_id)s
                    )
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    {
                        "message_id": message_id,
                        "handler_type": handler_type,
                        "execution_id": execution_id,
                    },
                )
            conn.commit()

    def cleanup_old_processed_messages(self, max_age_hours: float = 24.0) -> int:
        """Clean up old processed message records."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM processed_messages
                    WHERE processed_at < NOW() - INTERVAL '%(hours)s hours'
                    """,
                    {"hours": max_age_hours},
                )
                deleted = cur.rowcount
            conn.commit()
            return deleted

    # ========== Helper Methods ==========

    def _insert_stage(self, cur: Any, stage: StageExecution, execution_id: str) -> None:
        """Insert a stage."""
        cur.execute(
            """
            INSERT INTO stage_executions (
                id, execution_id, ref_id, type, name, status, context, outputs,
                requisite_stage_ref_ids, parent_stage_id, synthetic_stage_owner,
                start_time, end_time, start_time_expiry, scheduled_time
            ) VALUES (
                %(id)s, %(execution_id)s, %(ref_id)s, %(type)s, %(name)s, %(status)s,
                %(context)s::jsonb, %(outputs)s::jsonb, %(requisite_stage_ref_ids)s,
                %(parent_stage_id)s, %(synthetic_stage_owner)s, %(start_time)s,
                %(end_time)s, %(start_time_expiry)s, %(scheduled_time)s
            )
            """,
            {
                "id": stage.id,
                "execution_id": execution_id,
                "ref_id": stage.ref_id,
                "type": stage.type,
                "name": stage.name,
                "status": stage.status.name,
                "context": json.dumps(stage.context),
                "outputs": json.dumps(stage.outputs),
                "requisite_stage_ref_ids": list(stage.requisite_stage_ref_ids),
                "parent_stage_id": stage.parent_stage_id,
                "synthetic_stage_owner": (stage.synthetic_stage_owner.value if stage.synthetic_stage_owner else None),
                "start_time": stage.start_time,
                "end_time": stage.end_time,
                "start_time_expiry": stage.start_time_expiry,
                "scheduled_time": stage.scheduled_time,
            },
        )

        # Insert tasks
        for task in stage.tasks:
            self._upsert_task(cur, task, stage.id)

    def _upsert_task(self, cur: Any, task: TaskExecution, stage_id: str) -> None:
        """Insert or update a task."""
        cur.execute(
            """
            INSERT INTO task_executions (
                id, stage_id, name, implementing_class, status,
                start_time, end_time, stage_start, stage_end,
                loop_start, loop_end, task_exception_details
            ) VALUES (
                %(id)s, %(stage_id)s, %(name)s, %(implementing_class)s, %(status)s,
                %(start_time)s, %(end_time)s, %(stage_start)s, %(stage_end)s,
                %(loop_start)s, %(loop_end)s, %(task_exception_details)s::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                task_exception_details = EXCLUDED.task_exception_details
            """,
            {
                "id": task.id,
                "stage_id": stage_id,
                "name": task.name,
                "implementing_class": task.implementing_class,
                "status": task.status.name,
                "start_time": task.start_time,
                "end_time": task.end_time,
                "stage_start": task.stage_start,
                "stage_end": task.stage_end,
                "loop_start": task.loop_start,
                "loop_end": task.loop_end,
                "task_exception_details": json.dumps(task.task_exception_details),
            },
        )

    def _execution_to_dict(self, execution: Workflow) -> dict[str, Any]:
        """Convert execution to dictionary for storage."""
        return {
            "id": execution.id,
            "type": execution.type.value,
            "application": execution.application,
            "name": execution.name,
            "status": execution.status.name,
            "start_time": execution.start_time,
            "end_time": execution.end_time,
            "start_time_expiry": execution.start_time_expiry,
            "trigger": json.dumps(execution.trigger.to_dict()),
            "is_canceled": execution.is_canceled,
            "canceled_by": execution.canceled_by,
            "cancellation_reason": execution.cancellation_reason,
            "paused": (json.dumps(self._paused_to_dict(execution.paused)) if execution.paused else None),
            "pipeline_config_id": execution.pipeline_config_id,
            "is_limit_concurrent": execution.is_limit_concurrent,
            "max_concurrent_executions": execution.max_concurrent_executions,
            "keep_waiting_pipelines": execution.keep_waiting_pipelines,
            "origin": execution.origin,
        }

    def _paused_to_dict(self, paused: PausedDetails | None) -> dict[str, Any] | None:
        """Convert PausedDetails to dict."""
        if paused is None:
            return None
        return {
            "paused_by": paused.paused_by,
            "pause_time": paused.pause_time,
            "resume_time": paused.resume_time,
            "paused_ms": paused.paused_ms,
        }

    def _row_to_execution(self, row: dict[str, Any]) -> Workflow:
        """Convert database row to Workflow."""
        trigger_data = row["trigger"] if isinstance(row["trigger"], dict) else json.loads(row["trigger"] or "{}")
        paused_data = (
            row["paused"] if isinstance(row["paused"], dict) else json.loads(row["paused"]) if row["paused"] else None
        )

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
            start_time=row["start_time"],
            end_time=row["end_time"],
            start_time_expiry=row["start_time_expiry"],
            trigger=Trigger.from_dict(trigger_data),
            is_canceled=row["is_canceled"] or False,
            canceled_by=row["canceled_by"],
            cancellation_reason=row["cancellation_reason"],
            paused=paused,
            pipeline_config_id=row["pipeline_config_id"],
            is_limit_concurrent=row["is_limit_concurrent"] or False,
            max_concurrent_executions=row["max_concurrent_executions"] or 0,
            keep_waiting_pipelines=row["keep_waiting_pipelines"] or False,
            origin=row["origin"] or "unknown",
        )

    def _row_to_stage(self, row: dict[str, Any]) -> StageExecution:
        """Convert database row to StageExecution."""
        context = row["context"] if isinstance(row["context"], dict) else json.loads(row["context"] or "{}")
        outputs = row["outputs"] if isinstance(row["outputs"], dict) else json.loads(row["outputs"] or "{}")

        synthetic_owner = None
        if row["synthetic_stage_owner"]:
            synthetic_owner = SyntheticStageOwner(row["synthetic_stage_owner"])

        return StageExecution(
            id=row["id"],
            ref_id=row["ref_id"],
            type=row["type"],
            name=row["name"] or "",
            status=WorkflowStatus[row["status"]],
            context=context,
            outputs=outputs,
            requisite_stage_ref_ids=set(row["requisite_stage_ref_ids"] or []),
            parent_stage_id=row["parent_stage_id"],
            synthetic_stage_owner=synthetic_owner,
            start_time=row["start_time"],
            end_time=row["end_time"],
            start_time_expiry=row["start_time_expiry"],
            scheduled_time=row["scheduled_time"],
        )

    def _row_to_task(self, row: dict[str, Any]) -> TaskExecution:
        """Convert database row to TaskExecution."""
        exception_details = row["task_exception_details"]
        if isinstance(exception_details, str):
            exception_details = json.loads(exception_details or "{}")

        return TaskExecution(
            id=row["id"],
            name=row["name"],
            implementing_class=row["implementing_class"],
            status=WorkflowStatus[row["status"]],
            start_time=row["start_time"],
            end_time=row["end_time"],
            stage_start=row["stage_start"] or False,
            stage_end=row["stage_end"] or False,
            loop_start=row["loop_start"] or False,
            loop_end=row["loop_end"] or False,
            task_exception_details=exception_details or {},
        )

    def is_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception:
            return False
