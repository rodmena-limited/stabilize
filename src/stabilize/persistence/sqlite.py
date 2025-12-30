"""
SQLite execution repository.

Lightweight persistence using SQLite for development and small deployments.
Uses singleton ConnectionManager for efficient connection sharing.
"""

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
