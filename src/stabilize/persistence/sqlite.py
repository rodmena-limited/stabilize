from __future__ import annotations
import json
import sqlite3
import time
from collections.abc import Iterator
from typing import Any
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
