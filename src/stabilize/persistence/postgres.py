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
    def __init__(
        self,
        connection_string: str,
        create_tables: bool = False,
    ) -> None:
        """
        Initialize the repository.

        Args:
            connection_string: PostgreSQL connection string
            create_tables: Whether to create tables if they don't exist
        """
        from stabilize.persistence.connection import get_connection_manager

        self.connection_string = connection_string
        self._manager = get_connection_manager()
        self._pool = self._manager.get_postgres_pool(connection_string)

        if create_tables:
            self._create_tables()

    def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        schema = """
        CREATE TABLE IF NOT EXISTS pipeline_executions (
            id VARCHAR(26) PRIMARY KEY,
            type VARCHAR(50) NOT NULL,
            application VARCHAR(255) NOT NULL,
            name VARCHAR(255),
            status VARCHAR(50) NOT NULL,
            start_time BIGINT,
            end_time BIGINT,
            start_time_expiry BIGINT,
            trigger JSONB,
            is_canceled BOOLEAN DEFAULT FALSE,
            canceled_by VARCHAR(255),
            cancellation_reason TEXT,
            paused JSONB,
            pipeline_config_id VARCHAR(255),
            is_limit_concurrent BOOLEAN DEFAULT FALSE,
            max_concurrent_executions INT DEFAULT 0,
            keep_waiting_pipelines BOOLEAN DEFAULT FALSE,
            origin VARCHAR(255),
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS stage_executions (
            id VARCHAR(26) PRIMARY KEY,
            execution_id VARCHAR(26) REFERENCES pipeline_executions(id) ON DELETE CASCADE,
            ref_id VARCHAR(50) NOT NULL,
            type VARCHAR(100) NOT NULL,
            name VARCHAR(255),
            status VARCHAR(50) NOT NULL,
            context JSONB DEFAULT '{}',
            outputs JSONB DEFAULT '{}',
            requisite_stage_ref_ids TEXT[],
            parent_stage_id VARCHAR(26),
            synthetic_stage_owner VARCHAR(20),
            start_time BIGINT,
            end_time BIGINT,
            start_time_expiry BIGINT,
            scheduled_time BIGINT,
            UNIQUE(execution_id, ref_id)
        );

        CREATE TABLE IF NOT EXISTS task_executions (
            id VARCHAR(26) PRIMARY KEY,
            stage_id VARCHAR(26) REFERENCES stage_executions(id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            implementing_class VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            start_time BIGINT,
            end_time BIGINT,
            stage_start BOOLEAN DEFAULT FALSE,
            stage_end BOOLEAN DEFAULT FALSE,
            loop_start BOOLEAN DEFAULT FALSE,
            loop_end BOOLEAN DEFAULT FALSE,
            task_exception_details JSONB DEFAULT '{}'
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
        """

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                for statement in schema.split(";"):
                    statement = statement.strip()
                    if statement:
                        cur.execute(statement)
            conn.commit()

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
