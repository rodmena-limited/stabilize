"""
PostgreSQL execution repository.

Production-grade persistence using native psycopg3 with connection pooling.
Uses singleton ConnectionManager for efficient connection pool sharing.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from stabilize.models.workflow import Workflow
from stabilize.persistence.postgres.converters import (
    execution_to_dict,
    paused_to_dict,
    row_to_execution,
    row_to_stage,
    row_to_task,
)
from stabilize.persistence.postgres.helpers import insert_stage, upsert_tasks_bulk
from stabilize.persistence.postgres.operations import (
    cancel_execution,
    pause_execution,
    resume_execution,
)
from stabilize.persistence.postgres.operations import (
    cleanup_old_processed_messages as _cleanup_old_processed_messages,
)
from stabilize.persistence.postgres.operations import is_message_processed as _is_message_processed
from stabilize.persistence.postgres.operations import (
    mark_message_processed as _mark_message_processed,
)
from stabilize.persistence.postgres.queries import get_downstream_stages as _get_downstream_stages
from stabilize.persistence.postgres.queries import (
    get_merged_ancestor_outputs as _get_merged_ancestor_outputs,
)
from stabilize.persistence.postgres.queries import get_synthetic_stages as _get_synthetic_stages
from stabilize.persistence.postgres.queries import get_upstream_stages as _get_upstream_stages
from stabilize.persistence.postgres.queries import (
    load_tasks_for_stages,
)
from stabilize.persistence.postgres.queries import (
    retrieve_by_application as _retrieve_by_application,
)
from stabilize.persistence.postgres.queries import (
    retrieve_by_pipeline_config_id as _retrieve_by_pipeline_config_id,
)
from stabilize.persistence.store import (
    StoreTransaction,
    WorkflowCriteria,
    WorkflowNotFoundError,
    WorkflowStore,
)

logger = logging.getLogger(__name__)


class PostgresWorkflowStore(WorkflowStore):
    """
    PostgreSQL implementation of WorkflowStore.

    Uses native psycopg3 with connection pooling for database operations.
    Supports concurrent access and provides efficient queries for pipeline
    execution tracking.
    """

    def __init__(self, connection_string: str) -> None:
        """Initialize the repository."""
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
                cur.execute(
                    """
                    INSERT INTO pipeline_executions (
                        id, type, application, name, status, context, start_time, end_time,
                        start_time_expiry, trigger, is_canceled, canceled_by,
                        cancellation_reason, paused, pipeline_config_id,
                        is_limit_concurrent, max_concurrent_executions,
                        keep_waiting_pipelines, origin
                    ) VALUES (
                        %(id)s, %(type)s, %(application)s, %(name)s, %(status)s,
                        %(context)s::jsonb, %(start_time)s, %(end_time)s, %(start_time_expiry)s,
                        %(trigger)s::jsonb, %(is_canceled)s, %(canceled_by)s,
                        %(cancellation_reason)s, %(paused)s::jsonb, %(pipeline_config_id)s,
                        %(is_limit_concurrent)s, %(max_concurrent_executions)s,
                        %(keep_waiting_pipelines)s, %(origin)s
                    )
                    """,
                    execution_to_dict(execution),
                )

                for stage in execution.stages:
                    insert_stage(cur, stage, execution.id)

            conn.commit()

    def retrieve(self, execution_id: str) -> Workflow:
        """Retrieve an execution by ID."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM pipeline_executions WHERE id = %(id)s",
                    {"id": execution_id},
                )
                row = cur.fetchone()
                if not row:
                    raise WorkflowNotFoundError(execution_id)

                execution = row_to_execution(row)

                cur.execute(
                    "SELECT * FROM stage_executions WHERE execution_id = %(execution_id)s",
                    {"execution_id": execution_id},
                )

                stages_by_id: dict[str, Any] = {}
                stages: list[Any] = []
                for stage_row in cur.fetchall():
                    stage = row_to_stage(stage_row)
                    stage.execution = execution
                    stages_by_id[stage.id] = stage
                    stages.append(stage)

                if stages:
                    load_tasks_for_stages(cur, stages)

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

                return row_to_execution(row)

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
                        "paused": (json.dumps(paused_to_dict(execution.paused)) if execution.paused else None),
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

    def store_stage(
        self,
        stage: Any,
        expected_phase: str | None = None,
        connection: Any | None = None,
    ) -> None:
        """Store or update a stage.

        Args:
            stage: The stage to store
            expected_phase: If provided, adds status check to WHERE clause for
                           phase-aware optimistic locking.
            connection: Optional existing connection to use
        """
        if connection:
            with connection.cursor() as cur:
                self._store_stage_impl(cur, stage, expected_phase)
        else:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    self._store_stage_impl(cur, stage, expected_phase)
                conn.commit()

    def _store_stage_impl(
        self,
        cur: Any,
        stage: Any,
        expected_phase: str | None = None,
    ) -> None:
        """Implementation of store_stage using a cursor with optimistic locking."""
        from stabilize.errors import ConcurrencyError

        cur.execute(
            "SELECT id FROM stage_executions WHERE id = %(id)s",
            {"id": stage.id},
        )
        exists = cur.fetchone() is not None

        if exists:
            # Build update query with optimistic locking
            # Optionally include phase check
            if expected_phase is not None:
                cur.execute(
                    """
                    UPDATE stage_executions SET
                        status = %(status)s,
                        context = %(context)s::jsonb,
                        outputs = %(outputs)s::jsonb,
                        start_time = %(start_time)s,
                        end_time = %(end_time)s,
                        version = version + 1
                    WHERE id = %(id)s AND version = %(version)s AND status = %(expected_phase)s
                    RETURNING version
                    """,
                    {
                        "id": stage.id,
                        "status": stage.status.name,
                        "context": json.dumps(stage.context),
                        "outputs": json.dumps(stage.outputs),
                        "start_time": stage.start_time,
                        "end_time": stage.end_time,
                        "version": stage.version,
                        "expected_phase": expected_phase,
                    },
                )
            else:
                cur.execute(
                    """
                    UPDATE stage_executions SET
                        status = %(status)s,
                        context = %(context)s::jsonb,
                        outputs = %(outputs)s::jsonb,
                        start_time = %(start_time)s,
                        end_time = %(end_time)s,
                        version = version + 1
                    WHERE id = %(id)s AND version = %(version)s
                    RETURNING version
                    """,
                    {
                        "id": stage.id,
                        "status": stage.status.name,
                        "context": json.dumps(stage.context),
                        "outputs": json.dumps(stage.outputs),
                        "start_time": stage.start_time,
                        "end_time": stage.end_time,
                        "version": stage.version,
                    },
                )

            result = cur.fetchone()
            if result:
                new_version = result[0] if isinstance(result, tuple) else result.get("version", stage.version + 1)
                stage.version = new_version
            else:
                if expected_phase is not None:
                    raise ConcurrencyError(
                        f"Optimistic lock failed for stage {stage.id} "
                        f"(version {stage.version}, expected_phase {expected_phase}). "
                        f"Another process has modified this stage."
                    )
                raise ConcurrencyError(
                    f"Optimistic lock failed for stage {stage.id} (version {stage.version}). "
                    f"Another process has modified this stage."
                )

            if stage.tasks:
                upsert_tasks_bulk(cur, stage.tasks, stage.id)
        else:
            insert_stage(cur, stage, stage.execution.id)

    def add_stage(self, stage: Any) -> None:
        """Add a new stage."""
        self.store_stage(stage)

    def remove_stage(self, execution: Workflow, stage_id: str) -> None:
        """Remove a stage."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM stage_executions WHERE id = %(id)s",
                    {"id": stage_id},
                )
            conn.commit()

    def retrieve_stage(self, stage_id: str) -> Any:
        """Retrieve a single stage by ID."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM stage_executions WHERE id = %(id)s",
                    {"id": stage_id},
                )
                stage_row = cur.fetchone()
                if not stage_row:
                    raise ValueError(f"Stage {stage_id} not found")

                stage = row_to_stage(stage_row)

                cur.execute(
                    "SELECT * FROM pipeline_executions WHERE id = %(id)s",
                    {"id": stage_row["execution_id"]},
                )
                exec_row = cur.fetchone()
                if exec_row:
                    execution = row_to_execution(exec_row)
                    stage.set_execution_strong(execution)

                    all_stages = [stage]
                    requisites = list(stage.requisite_stage_ref_ids)
                    if requisites:
                        cur.execute(
                            """
                            SELECT * FROM stage_executions
                            WHERE execution_id = %(execution_id)s
                            AND ref_id = ANY(%(requisites)s)
                            """,
                            {"execution_id": execution.id, "requisites": requisites},
                        )
                        for us_row in cur.fetchall():
                            us = row_to_stage(us_row)
                            us.set_execution_strong(execution)
                            all_stages.append(us)

                    synthetic_stages = self.get_synthetic_stages(execution.id, stage.id)
                    for ss in synthetic_stages:
                        ss.set_execution_strong(execution)
                        all_stages.append(ss)

                    execution.stages = all_stages

                # ORDER BY id ensures consistent task sequencing (ULID encodes creation time)
                cur.execute(
                    "SELECT * FROM task_executions WHERE stage_id = %(stage_id)s ORDER BY id ASC",
                    {"stage_id": stage.id},
                )
                for task_row in cur.fetchall():
                    task = row_to_task(task_row)
                    task.stage = stage
                    stage.tasks.append(task)

                return stage

    def get_upstream_stages(self, execution_id: str, stage_ref_id: str) -> list[Any]:
        """Get upstream stages with tasks loaded."""
        return _get_upstream_stages(self._pool, execution_id, stage_ref_id)

    def get_downstream_stages(self, execution_id: str, stage_ref_id: str) -> list[Any]:
        """Get downstream stages with tasks loaded."""
        return _get_downstream_stages(self._pool, execution_id, stage_ref_id)

    def get_synthetic_stages(self, execution_id: str, parent_stage_id: str) -> list[Any]:
        """Get synthetic stages with tasks loaded."""
        return _get_synthetic_stages(self._pool, execution_id, parent_stage_id)

    def get_merged_ancestor_outputs(self, execution_id: str, stage_ref_id: str) -> dict[str, Any]:
        """Get merged outputs from all ancestor stages."""
        return _get_merged_ancestor_outputs(self._pool, execution_id, stage_ref_id)

    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by pipeline config ID."""
        return _retrieve_by_pipeline_config_id(self._pool, pipeline_config_id, criteria, self.retrieve)

    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by application."""
        return _retrieve_by_application(self._pool, application, criteria, self.retrieve)

    def pause(self, execution_id: str, paused_by: str) -> None:
        """Pause an execution."""
        pause_execution(self._pool, execution_id, paused_by)

    def resume(self, execution_id: str) -> None:
        """Resume a paused execution."""
        resume_execution(self._pool, execution_id)

    def cancel(self, execution_id: str, canceled_by: str, reason: str) -> None:
        """Cancel an execution."""
        cancel_execution(self._pool, execution_id, canceled_by, reason)

    def is_message_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed."""
        return _is_message_processed(self._pool, message_id)

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed."""
        _mark_message_processed(self._pool, message_id, handler_type, execution_id)

    def cleanup_old_processed_messages(self, max_age_hours: float = 24.0) -> int:
        """Clean up old processed message records."""
        return _cleanup_old_processed_messages(self._pool, max_age_hours)

    def is_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    @contextmanager
    def transaction(self, queue: Any | None = None) -> Iterator[StoreTransaction]:
        """Create an atomic transaction for store + queue operations."""
        from stabilize.persistence.postgres.transaction import PostgresTransaction

        with self._pool.connection() as conn:
            txn = PostgresTransaction(conn, self, queue)
            try:
                yield txn
                conn.commit()
            except Exception:
                conn.rollback()
                txn.rollback_versions()
                raise
