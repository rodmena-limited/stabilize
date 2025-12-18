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
