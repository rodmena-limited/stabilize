"""
SQLite execution repository.

Lightweight persistence using SQLite for development and small deployments.
Uses singleton ConnectionManager for efficient connection sharing.

Enterprise Features:
- Atomic transactions for store + queue operations
- Dead letter queue support
- Thread-safe connection management via WAL mode
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

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
    StoreTransaction,
    WorkflowCriteria,
    WorkflowNotFoundError,
    WorkflowStore,
)

if TYPE_CHECKING:
    from stabilize.queue.messages import Message
    from stabilize.queue.queue import Queue


class SqliteWorkflowStore(WorkflowStore):
    """
    SQLite implementation of WorkflowStore.

    Uses native sqlite3 for file-based or in-memory storage.
    Suitable for development, testing, and single-node deployments.

    Features:
    - WAL mode for better concurrent read performance
    - Foreign key support enabled
    - JSON stored as TEXT strings
    - Arrays stored as JSON strings
    - Thread-local connections managed by singleton ConnectionManager
    """

    def __init__(
        self,
        connection_string: str,
        create_tables: bool = False,
    ) -> None:
        """
        Initialize the repository.

        Args:
            connection_string: SQLite connection string (e.g., sqlite:///./db.sqlite)
            create_tables: Whether to create tables if they don't exist
        """
        from stabilize.persistence.connection import get_connection_manager

        self.connection_string = connection_string
        self._manager = get_connection_manager()

        # Verify connection works
        conn = self._get_connection()
        conn.execute("SELECT 1")

        if create_tables:
            self._create_tables()

    def _get_connection(self) -> sqlite3.Connection:
        """
        Get thread-local connection from ConnectionManager.

        Returns a connection configured with:
        - Row factory for dict-like access
        - Foreign keys enabled
        - WAL journal mode for concurrency
        - 30 second busy timeout
        """
        return self._manager.get_sqlite_connection(self.connection_string)

    def close(self) -> None:
        """Close SQLite connection for current thread."""
        self._manager.close_sqlite_connection(self.connection_string)

    def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        schema = """
        CREATE TABLE IF NOT EXISTS pipeline_executions (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            application TEXT NOT NULL,
            name TEXT,
            status TEXT NOT NULL,
            start_time INTEGER,
            end_time INTEGER,
            start_time_expiry INTEGER,
            trigger TEXT,
            is_canceled INTEGER DEFAULT 0,
            canceled_by TEXT,
            cancellation_reason TEXT,
            paused TEXT,
            pipeline_config_id TEXT,
            is_limit_concurrent INTEGER DEFAULT 0,
            max_concurrent_executions INTEGER DEFAULT 0,
            keep_waiting_pipelines INTEGER DEFAULT 0,
            origin TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS stage_executions (
            id TEXT PRIMARY KEY,
            execution_id TEXT NOT NULL REFERENCES pipeline_executions(id) ON DELETE CASCADE,
            ref_id TEXT NOT NULL,
            type TEXT NOT NULL,
            name TEXT,
            status TEXT NOT NULL,
            context TEXT DEFAULT '{}',
            outputs TEXT DEFAULT '{}',
            requisite_stage_ref_ids TEXT,
            parent_stage_id TEXT,
            synthetic_stage_owner TEXT,
            start_time INTEGER,
            end_time INTEGER,
            start_time_expiry INTEGER,
            scheduled_time INTEGER,
            UNIQUE(execution_id, ref_id)
        );

        CREATE TABLE IF NOT EXISTS task_executions (
            id TEXT PRIMARY KEY,
            stage_id TEXT NOT NULL REFERENCES stage_executions(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            implementing_class TEXT NOT NULL,
            status TEXT NOT NULL,
            start_time INTEGER,
            end_time INTEGER,
            stage_start INTEGER DEFAULT 0,
            stage_end INTEGER DEFAULT 0,
            loop_start INTEGER DEFAULT 0,
            loop_end INTEGER DEFAULT 0,
            task_exception_details TEXT DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS queue_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            message_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            deliver_at TEXT NOT NULL DEFAULT (datetime('now')),
            attempts INTEGER DEFAULT 0,
            max_attempts INTEGER DEFAULT 10,
            locked_until TEXT,
            version INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_execution_application
            ON pipeline_executions(application);
        CREATE INDEX IF NOT EXISTS idx_execution_config
            ON pipeline_executions(pipeline_config_id);
        CREATE INDEX IF NOT EXISTS idx_execution_status
            ON pipeline_executions(status);
        CREATE INDEX IF NOT EXISTS idx_stage_execution
            ON stage_executions(execution_id);
        CREATE INDEX IF NOT EXISTS idx_task_stage
            ON task_executions(stage_id);
        CREATE INDEX IF NOT EXISTS idx_queue_deliver
            ON queue_messages(deliver_at);
        CREATE INDEX IF NOT EXISTS idx_queue_locked
            ON queue_messages(locked_until);

        CREATE TABLE IF NOT EXISTS processed_messages (
            message_id TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL DEFAULT (datetime('now')),
            handler_type TEXT,
            execution_id TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_processed_messages_at
            ON processed_messages(processed_at);
        """

        conn = self._get_connection()
        for statement in schema.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(statement)
        conn.commit()

    def store(self, execution: Workflow) -> None:
        """Store a complete execution."""
        conn = self._get_connection()

        # Insert execution
        conn.execute(
            """
            INSERT INTO pipeline_executions (
                id, type, application, name, status, start_time, end_time,
                start_time_expiry, trigger, is_canceled, canceled_by,
                cancellation_reason, paused, pipeline_config_id,
                is_limit_concurrent, max_concurrent_executions,
                keep_waiting_pipelines, origin
            ) VALUES (
                :id, :type, :application, :name, :status, :start_time, :end_time,
                :start_time_expiry, :trigger, :is_canceled, :canceled_by,
                :cancellation_reason, :paused, :pipeline_config_id,
                :is_limit_concurrent, :max_concurrent_executions,
                :keep_waiting_pipelines, :origin
            )
            """,
            self._execution_to_dict(execution),
        )

        # Insert stages
        for stage in execution.stages:
            self._insert_stage(conn, stage, execution.id)

        conn.commit()

    def retrieve(self, execution_id: str) -> Workflow:
        """Retrieve an execution by ID."""
        conn = self._get_connection()

        # Get execution
        result = conn.execute(
            "SELECT * FROM pipeline_executions WHERE id = :id",
            {"id": execution_id},
        )
        row = result.fetchone()
        if not row:
            raise WorkflowNotFoundError(execution_id)

        execution = self._row_to_execution(row)

        # Get stages
        result = conn.execute(
            """
            SELECT * FROM stage_executions
            WHERE execution_id = :execution_id
            """,
            {"execution_id": execution_id},
        )
        stages = []
        for stage_row in result.fetchall():
            stage = self._row_to_stage(stage_row)
            stage._execution = execution

            # Get tasks for stage
            task_result = conn.execute(
                """
                SELECT * FROM task_executions
                WHERE stage_id = :stage_id
                """,
                {"stage_id": stage.id},
            )
            for task_row in task_result.fetchall():
                task = self._row_to_task(task_row)
                task._stage = stage
                stage.tasks.append(task)

            stages.append(stage)

        execution.stages = stages
        return execution

    def retrieve_execution_summary(self, execution_id: str) -> Workflow:
        """Retrieve execution metadata without stages."""
        conn = self._get_connection()
        result = conn.execute(
            "SELECT * FROM pipeline_executions WHERE id = :id",
            {"id": execution_id},
        )
        row = result.fetchone()
        if not row:
            raise WorkflowNotFoundError(execution_id)

        return self._row_to_execution(row)

    def update_status(self, execution: Workflow) -> None:
        """Update execution status."""
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE pipeline_executions SET
                status = :status,
                start_time = :start_time,
                end_time = :end_time,
                is_canceled = :is_canceled,
                canceled_by = :canceled_by,
                cancellation_reason = :cancellation_reason,
                paused = :paused
            WHERE id = :id
            """,
            {
                "id": execution.id,
                "status": execution.status.name,
                "start_time": execution.start_time,
                "end_time": execution.end_time,
                "is_canceled": 1 if execution.is_canceled else 0,
                "canceled_by": execution.canceled_by,
                "cancellation_reason": execution.cancellation_reason,
                "paused": (json.dumps(self._paused_to_dict(execution.paused)) if execution.paused else None),
            },
        )
        conn.commit()

    def delete(self, execution_id: str) -> None:
        """Delete an execution."""
        conn = self._get_connection()
        conn.execute(
            "DELETE FROM pipeline_executions WHERE id = :id",
            {"id": execution_id},
        )
        conn.commit()

    def store_stage(self, stage: StageExecution) -> None:
        """Store or update a stage."""
        conn = self._get_connection()

        # Check if stage exists
        result = conn.execute(
            "SELECT id FROM stage_executions WHERE id = :id",
            {"id": stage.id},
        )
        exists = result.fetchone() is not None

        if exists:
            # Update
            conn.execute(
                """
                UPDATE stage_executions SET
                    status = :status,
                    context = :context,
                    outputs = :outputs,
                    start_time = :start_time,
                    end_time = :end_time
                WHERE id = :id
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
                self._upsert_task(conn, task, stage.id)
        else:
            self._insert_stage(conn, stage, stage.execution.id)

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
        conn = self._get_connection()
        conn.execute(
            "DELETE FROM stage_executions WHERE id = :id",
            {"id": stage_id},
        )
        conn.commit()

    def retrieve_stage(self, stage_id: str) -> StageExecution:
        """Retrieve a single stage by ID."""
        conn = self._get_connection()

        # Get stage
        result = conn.execute(
            "SELECT * FROM stage_executions WHERE id = :id",
            {"id": stage_id},
        )
        stage_row = result.fetchone()
        if not stage_row:
            raise ValueError(f"Stage {stage_id} not found")

        stage = self._row_to_stage(stage_row)

        # Get execution summary for context
        exec_result = conn.execute(
            "SELECT * FROM pipeline_executions WHERE id = :id",
            {"id": stage_row["execution_id"]},
        )
        exec_row = exec_result.fetchone()
        if exec_row:
            execution = self._row_to_execution(exec_row)
            stage._execution = execution
            # Add stage to execution's stage list so it can be found
            execution.stages = [stage]

        # Get tasks
        task_result = conn.execute(
            """
            SELECT * FROM task_executions
            WHERE stage_id = :stage_id
            """,
            {"stage_id": stage.id},
        )
        for task_row in task_result.fetchall():
            task = self._row_to_task(task_row)
            task._stage = stage
            stage.tasks.append(task)

        return stage

    def get_upstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get upstream stages."""
        conn = self._get_connection()

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
        params = {"execution_id": execution_id}
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
            stage = self._row_to_stage(stage_row)
            stages.append(stage)

        return stages

    def get_downstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get downstream stages."""
        conn = self._get_connection()

        # Use LIKE for JSON array membership test as it's portable and safe enough here
        # (Assuming standard formatting of ["a", "b"])
        # Alternatively, we could use json_each if available, but fallback to LIKE is safer for older sqlite
        # For this codebase (Python 3.11), json_each is available.

        try:
            query = """
                SELECT stage_executions.* FROM stage_executions, json_each(stage_executions.requisite_stage_ref_ids)
                WHERE execution_id = :execution_id
                AND json_each.value = :ref_id
            """
            result = conn.execute(query, {"execution_id": execution_id, "ref_id": stage_ref_id})
        except sqlite3.OperationalError:
            # Fallback if json_each is not enabled (unlikely in 3.11 but safe)
            # "ref_id" inside ["..."]
            # Match "%"ref_id"%"
            json_ref_id = json.dumps(stage_ref_id)
            query = """
                SELECT * FROM stage_executions
                WHERE execution_id = :execution_id
                AND requisite_stage_ref_ids LIKE :pattern
             """
            result = conn.execute(query, {"execution_id": execution_id, "pattern": f"%{json_ref_id}%"})

        stages = []
        for stage_row in result.fetchall():
            stage = self._row_to_stage(stage_row)
            stages.append(stage)

        return stages

    def get_synthetic_stages(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[StageExecution]:
        """Get synthetic stages."""
        conn = self._get_connection()
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
            stage = self._row_to_stage(stage_row)
            stages.append(stage)

        return stages

    def get_merged_ancestor_outputs(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> dict[str, Any]:
        """Get merged outputs from all ancestor stages."""
        # Fetch lightweight graph (ref_id, requisites, outputs)
        conn = self._get_connection()
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
                if key in merged_result and isinstance(merged_result[key], list) and isinstance(value, list):
                    # Concatenate lists
                    existing = merged_result[key]
                    for item in value:
                        if item not in existing:
                            existing.append(item)
                else:
                    merged_result[key] = value

        return merged_result

    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
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

        conn = self._get_connection()
        result = conn.execute(query, params)
        for row in result.fetchall():
            yield self.retrieve(row[0])

    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
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

        conn = self._get_connection()
        result = conn.execute(query, params)
        for row in result.fetchall():
            yield self.retrieve(row[0])

    def pause(self, execution_id: str, paused_by: str) -> None:
        """Pause an execution."""
        paused = PausedDetails(
            paused_by=paused_by,
            pause_time=int(time.time() * 1000),
        )

        conn = self._get_connection()
        conn.execute(
            """
            UPDATE pipeline_executions SET
                status = :status,
                paused = :paused
            WHERE id = :id
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

        conn = self._get_connection()
        conn.execute(
            """
            UPDATE pipeline_executions SET
                status = :status,
                paused = :paused
            WHERE id = :id
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
        conn = self._get_connection()
        conn.execute(
            """
            UPDATE pipeline_executions SET
                is_canceled = 1,
                canceled_by = :canceled_by,
                cancellation_reason = :reason
            WHERE id = :id
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
        conn = self._get_connection()
        result = conn.execute(
            "SELECT 1 FROM processed_messages WHERE message_id = :message_id",
            {"message_id": message_id},
        ).fetchone()
        return result is not None

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed."""
        conn = self._get_connection()
        conn.execute(
            """
            INSERT OR IGNORE INTO processed_messages (
                message_id, processed_at, handler_type, execution_id
            ) VALUES (
                :message_id, datetime('now'), :handler_type, :execution_id
            )
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
        conn = self._get_connection()
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        cursor = conn.execute(
            "DELETE FROM processed_messages WHERE processed_at < :cutoff",
            {"cutoff": cutoff.isoformat()},
        )
        conn.commit()
        return cursor.rowcount

    # ========== Helper Methods ==========

    def _insert_stage(self, conn: sqlite3.Connection, stage: StageExecution, execution_id: str) -> None:
        """Insert a stage."""
        conn.execute(
            """
            INSERT INTO stage_executions (
                id, execution_id, ref_id, type, name, status, context, outputs,
                requisite_stage_ref_ids, parent_stage_id, synthetic_stage_owner,
                start_time, end_time, start_time_expiry, scheduled_time
            ) VALUES (
                :id, :execution_id, :ref_id, :type, :name, :status,
                :context, :outputs, :requisite_stage_ref_ids,
                :parent_stage_id, :synthetic_stage_owner, :start_time,
                :end_time, :start_time_expiry, :scheduled_time
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
                "requisite_stage_ref_ids": json.dumps(list(stage.requisite_stage_ref_ids)),
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
            self._upsert_task(conn, task, stage.id)

    def _upsert_task(self, conn: sqlite3.Connection, task: TaskExecution, stage_id: str) -> None:
        """Insert or update a task."""
        conn.execute(
            """
            INSERT OR REPLACE INTO task_executions (
                id, stage_id, name, implementing_class, status,
                start_time, end_time, stage_start, stage_end,
                loop_start, loop_end, task_exception_details
            ) VALUES (
                :id, :stage_id, :name, :implementing_class, :status,
                :start_time, :end_time, :stage_start, :stage_end,
                :loop_start, :loop_end, :task_exception_details
            )
            """,
            {
                "id": task.id,
                "stage_id": stage_id,
                "name": task.name,
                "implementing_class": task.implementing_class,
                "status": task.status.name,
                "start_time": task.start_time,
                "end_time": task.end_time,
                "stage_start": 1 if task.stage_start else 0,
                "stage_end": 1 if task.stage_end else 0,
                "loop_start": 1 if task.loop_start else 0,
                "loop_end": 1 if task.loop_end else 0,
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
            "is_canceled": 1 if execution.is_canceled else 0,
            "canceled_by": execution.canceled_by,
            "cancellation_reason": execution.cancellation_reason,
            "paused": (json.dumps(self._paused_to_dict(execution.paused)) if execution.paused else None),
            "pipeline_config_id": execution.pipeline_config_id,
            "is_limit_concurrent": 1 if execution.is_limit_concurrent else 0,
            "max_concurrent_executions": execution.max_concurrent_executions,
            "keep_waiting_pipelines": 1 if execution.keep_waiting_pipelines else 0,
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

    def _row_to_execution(self, row: sqlite3.Row) -> Workflow:
        """Convert database row to Workflow."""
        trigger_data = json.loads(row["trigger"] or "{}")
        paused_data = json.loads(row["paused"]) if row["paused"] else None

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
            is_canceled=bool(row["is_canceled"]),
            canceled_by=row["canceled_by"],
            cancellation_reason=row["cancellation_reason"],
            paused=paused,
            pipeline_config_id=row["pipeline_config_id"],
            is_limit_concurrent=bool(row["is_limit_concurrent"]),
            max_concurrent_executions=row["max_concurrent_executions"] or 0,
            keep_waiting_pipelines=bool(row["keep_waiting_pipelines"]),
            origin=row["origin"] or "unknown",
        )

    def _row_to_stage(self, row: sqlite3.Row) -> StageExecution:
        """Convert database row to StageExecution."""
        context = json.loads(row["context"] or "{}")
        outputs = json.loads(row["outputs"] or "{}")
        requisite_ids = json.loads(row["requisite_stage_ref_ids"] or "[]")

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
            requisite_stage_ref_ids=set(requisite_ids),
            parent_stage_id=row["parent_stage_id"],
            synthetic_stage_owner=synthetic_owner,
            start_time=row["start_time"],
            end_time=row["end_time"],
            start_time_expiry=row["start_time_expiry"],
            scheduled_time=row["scheduled_time"],
        )

    def _row_to_task(self, row: sqlite3.Row) -> TaskExecution:
        """Convert database row to TaskExecution."""
        exception_details = json.loads(row["task_exception_details"] or "{}")

        return TaskExecution(
            id=row["id"],
            name=row["name"],
            implementing_class=row["implementing_class"],
            status=WorkflowStatus[row["status"]],
            start_time=row["start_time"],
            end_time=row["end_time"],
            stage_start=bool(row["stage_start"]),
            stage_end=bool(row["stage_end"]),
            loop_start=bool(row["loop_start"]),
            loop_end=bool(row["loop_end"]),
            task_exception_details=exception_details,
        )

    def is_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            conn = self._get_connection()
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    @contextmanager
    def transaction(self, queue: Queue | None = None) -> Iterator[AtomicTransaction]:
        """Create atomic transaction for store + queue operations.

        Use this when you need to atomically update both stage state AND
        queue a message. This prevents orphaned workflows from crashes
        between separate store and queue operations.

        SQLite implementation writes directly to the queue_messages table
        in the same transaction, so the queue parameter is ignored.

        Args:
            queue: Ignored (for API compatibility with base class)

        Usage:
            with store.transaction() as txn:
                txn.store_stage(stage)
                txn.push_message(message)
            # Auto-commits on success, rolls back on exception

        Yields:
            AtomicTransaction with store_stage() and push_message() methods
        """
        conn = self._get_connection()
        txn = AtomicTransaction(conn, self)
        try:
            yield txn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def list_workflows(
        self,
        application: str,
        limit: int = 100,
    ) -> list[Workflow]:
        """List workflows for an application.

        Args:
            application: Application name to filter by
            limit: Maximum number of workflows to return

        Returns:
            List of Workflow objects
        """
        conn = self._get_connection()
        result = conn.execute(
            """
            SELECT * FROM pipeline_executions
            WHERE application = :application
            ORDER BY created_at DESC
            LIMIT :limit
            """,
            {"application": application, "limit": limit},
        )

        workflows = []
        for row in result.fetchall():
            workflows.append(self._row_to_execution(row))
        return workflows


class AtomicTransaction(StoreTransaction):
    """Atomic transaction spanning store and queue operations.

    This class ensures that stage updates and message pushes happen
    atomically. If either operation fails, both are rolled back.

    This solves the critical issue where:
    1. store_stage() succeeds
    2. push_message() fails (or crash)
    3. Workflow is stuck forever (stage saved but no message to continue)

    With AtomicTransaction, both operations use the same SQLite connection
    and commit only when both succeed.
    """

    def __init__(self, conn: sqlite3.Connection, store: SqliteWorkflowStore) -> None:
        """Initialize atomic transaction.

        Args:
            conn: SQLite connection (will not auto-commit)
            store: Parent store for helper methods
        """
        self._conn = conn
        self._store = store

    def store_stage(self, stage: StageExecution) -> None:
        """Store or update a stage within the transaction.

        This performs the same operations as SqliteWorkflowStore.store_stage()
        but does NOT commit - the commit happens when the transaction
        context manager exits successfully.

        Args:
            stage: Stage to store/update
        """
        # Check if stage exists
        result = self._conn.execute(
            "SELECT id FROM stage_executions WHERE id = :id",
            {"id": stage.id},
        )
        exists = result.fetchone() is not None

        if exists:
            # Update
            self._conn.execute(
                """
                UPDATE stage_executions SET
                    status = :status,
                    context = :context,
                    outputs = :outputs,
                    start_time = :start_time,
                    end_time = :end_time
                WHERE id = :id
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
                self._upsert_task(task, stage.id)
        else:
            self._insert_stage(stage, stage.execution.id)

    def push_message(
        self,
        message: Message,
        delay: int = 0,
    ) -> None:
        """Push a message to the queue within the transaction.

        This inserts directly into queue_messages table using the same
        connection as store_stage(), ensuring atomicity.

        Args:
            message: Message to push
            delay: Delay in seconds before message is deliverable
        """
        from enum import Enum

        from stabilize.queue.messages import get_message_type_name

        deliver_at = datetime.now()
        if delay > 0:
            deliver_at = deliver_at + timedelta(seconds=delay)

        # Generate message_id if not present
        message_id = getattr(message, "id", None) or str(uuid.uuid4())

        # Get message type name using the helper function
        message_type = get_message_type_name(message)

        # Serialize message (same logic as SqliteQueue._serialize_message)
        data = {}
        for key, value in message.__dict__.items():
            if key.startswith("_"):
                continue
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif isinstance(value, Enum):
                data[key] = value.name
            else:
                data[key] = value
        payload = json.dumps(data)

        self._conn.execute(
            """
            INSERT INTO queue_messages (
                message_id, message_type, payload, deliver_at,
                attempts, max_attempts, version
            ) VALUES (
                :message_id, :message_type, :payload, :deliver_at,
                0, :max_attempts, 0
            )
            """,
            {
                "message_id": message_id,
                "message_type": message_type,
                "payload": payload,
                "deliver_at": deliver_at.isoformat(),
                "max_attempts": getattr(message, "max_attempts", 10),
            },
        )

    def _insert_stage(self, stage: StageExecution, execution_id: str) -> None:
        """Insert a stage (no commit)."""
        self._conn.execute(
            """
            INSERT INTO stage_executions (
                id, execution_id, ref_id, type, name, status, context, outputs,
                requisite_stage_ref_ids, parent_stage_id, synthetic_stage_owner,
                start_time, end_time, start_time_expiry, scheduled_time
            ) VALUES (
                :id, :execution_id, :ref_id, :type, :name, :status,
                :context, :outputs, :requisite_stage_ref_ids,
                :parent_stage_id, :synthetic_stage_owner, :start_time,
                :end_time, :start_time_expiry, :scheduled_time
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
                "requisite_stage_ref_ids": json.dumps(list(stage.requisite_stage_ref_ids)),
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
            self._upsert_task(task, stage.id)

    def _upsert_task(self, task: TaskExecution, stage_id: str) -> None:
        """Insert or update a task (no commit)."""
        self._conn.execute(
            """
            INSERT OR REPLACE INTO task_executions (
                id, stage_id, name, implementing_class, status,
                start_time, end_time, stage_start, stage_end,
                loop_start, loop_end, task_exception_details
            ) VALUES (
                :id, :stage_id, :name, :implementing_class, :status,
                :start_time, :end_time, :stage_start, :stage_end,
                :loop_start, :loop_end, :task_exception_details
            )
            """,
            {
                "id": task.id,
                "stage_id": stage_id,
                "name": task.name,
                "implementing_class": task.implementing_class,
                "status": task.status.name,
                "start_time": task.start_time,
                "end_time": task.end_time,
                "stage_start": 1 if task.stage_start else 0,
                "stage_end": 1 if task.stage_end else 0,
                "loop_start": 1 if task.loop_start else 0,
                "loop_end": 1 if task.loop_end else 0,
                "task_exception_details": json.dumps(task.task_exception_details),
            },
        )
