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
import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from stabilize.errors import ConcurrencyError
from stabilize.models.workflow import Workflow
from stabilize.persistence.sqlite.converters import (
    execution_to_dict,
    paused_to_dict,
    row_to_execution,
    row_to_stage,
    row_to_task,
)
from stabilize.persistence.sqlite.helpers import insert_stage, upsert_task
from stabilize.persistence.sqlite.operations import (
    cancel_execution,
    pause_execution,
    resume_execution,
)
from stabilize.persistence.sqlite.operations import (
    cleanup_old_processed_messages as _cleanup_old_processed_messages,
)
from stabilize.persistence.sqlite.operations import is_message_processed as _is_message_processed
from stabilize.persistence.sqlite.operations import (
    mark_message_processed as _mark_message_processed,
)
from stabilize.persistence.sqlite.queries import get_downstream_stages as _get_downstream_stages
from stabilize.persistence.sqlite.queries import (
    get_merged_ancestor_outputs as _get_merged_ancestor_outputs,
)
from stabilize.persistence.sqlite.queries import get_synthetic_stages as _get_synthetic_stages
from stabilize.persistence.sqlite.queries import get_upstream_stages as _get_upstream_stages
from stabilize.persistence.sqlite.queries import (
    retrieve_by_application as _retrieve_by_application,
)
from stabilize.persistence.sqlite.queries import (
    retrieve_by_pipeline_config_id as _retrieve_by_pipeline_config_id,
)
from stabilize.persistence.sqlite.schema import create_tables
from stabilize.persistence.store import (
    WorkflowCriteria,
    WorkflowNotFoundError,
    WorkflowStore,
)

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.persistence.store import StoreTransaction
    from stabilize.queue import Queue

logger = logging.getLogger(__name__)


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
        create_tables(self._get_connection())

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
            execution_to_dict(execution),
        )

        # Insert stages
        for stage in execution.stages:
            insert_stage(conn, stage, execution.id)

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

        execution = row_to_execution(row)

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
            stage = row_to_stage(stage_row)
            stage.execution = execution

            # Get tasks for stage
            task_result = conn.execute(
                """
                SELECT * FROM task_executions
                WHERE stage_id = :stage_id
                """,
                {"stage_id": stage.id},
            )
            for task_row in task_result.fetchall():
                task = row_to_task(task_row)
                task.stage = stage
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

        return row_to_execution(row)

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
                "paused": (json.dumps(paused_to_dict(execution.paused)) if execution.paused else None),
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
            # Update with optimistic locking
            cursor = conn.execute(
                """
                UPDATE stage_executions SET
                    status = :status,
                    context = :context,
                    outputs = :outputs,
                    start_time = :start_time,
                    end_time = :end_time,
                    version = version + 1
                WHERE id = :id AND version = :version
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

            if cursor.rowcount == 0:
                raise ConcurrencyError(f"Optimistic lock failed for stage {stage.id} (version {stage.version})")

            # Update local version
            stage.version += 1

            # Update tasks
            for task in stage.tasks:
                upsert_task(conn, task, stage.id)
        else:
            insert_stage(conn, stage, stage.execution.id)

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

        stage = row_to_stage(stage_row)

        # Get execution summary for context
        exec_result = conn.execute(
            "SELECT * FROM pipeline_executions WHERE id = :id",
            {"id": stage_row["execution_id"]},
        )
        exec_row = exec_result.fetchone()
        if exec_row:
            execution = row_to_execution(exec_row)
            # Use strong reference because we are returning the stage
            stage.set_execution_strong(execution)

            # Load current stage AND its upstream stages so upstream_stages() works
            # This is critical for tasks that need to access upstream stage outputs
            all_stages = [stage]
            upstream_stages = self.get_upstream_stages(execution.id, stage.ref_id)
            for us in upstream_stages:
                # Upstream stages should also hold strong ref to this partial execution context
                us.set_execution_strong(execution)
                all_stages.append(us)

            # Also load synthetic stages (children) so after_stages() works
            # This is critical for ContinueParentStageHandler to find after-stages
            synthetic_stages = self.get_synthetic_stages(execution.id, stage.id)
            for ss in synthetic_stages:
                ss.set_execution_strong(execution)
                all_stages.append(ss)

            execution.stages = all_stages

        # Get tasks
        task_result = conn.execute(
            """
            SELECT * FROM task_executions
            WHERE stage_id = :stage_id
            """,
            {"stage_id": stage.id},
        )
        for task_row in task_result.fetchall():
            task = row_to_task(task_row)
            task.stage = stage
            stage.tasks.append(task)

        return stage

    def get_upstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get upstream stages with tasks loaded."""
        return _get_upstream_stages(self._get_connection(), execution_id, stage_ref_id)

    def get_downstream_stages(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> list[StageExecution]:
        """Get downstream stages with tasks loaded."""
        return _get_downstream_stages(self._get_connection(), execution_id, stage_ref_id)

    def get_synthetic_stages(
        self,
        execution_id: str,
        parent_stage_id: str,
    ) -> list[StageExecution]:
        """Get synthetic stages with tasks loaded."""
        return _get_synthetic_stages(self._get_connection(), execution_id, parent_stage_id)

    def get_merged_ancestor_outputs(
        self,
        execution_id: str,
        stage_ref_id: str,
    ) -> dict[str, Any]:
        """Get merged outputs from all ancestor stages."""
        return _get_merged_ancestor_outputs(self._get_connection(), execution_id, stage_ref_id)

    def retrieve_by_pipeline_config_id(
        self,
        pipeline_config_id: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by pipeline config ID."""
        return _retrieve_by_pipeline_config_id(self._get_connection(), pipeline_config_id, criteria, self.retrieve)

    def retrieve_by_application(
        self,
        application: str,
        criteria: WorkflowCriteria | None = None,
    ) -> Iterator[Workflow]:
        """Retrieve executions by application."""
        return _retrieve_by_application(self._get_connection(), application, criteria, self.retrieve)

    def pause(self, execution_id: str, paused_by: str) -> None:
        """Pause an execution."""
        pause_execution(self._get_connection(), execution_id, paused_by)

    def resume(self, execution_id: str) -> None:
        """Resume a paused execution."""
        resume_execution(self._get_connection(), execution_id)

    def cancel(
        self,
        execution_id: str,
        canceled_by: str,
        reason: str,
    ) -> None:
        """Cancel an execution."""
        cancel_execution(self._get_connection(), execution_id, canceled_by, reason)

    # ========== Message Deduplication ==========

    def is_message_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed."""
        return _is_message_processed(self._get_connection(), message_id)

    def mark_message_processed(
        self,
        message_id: str,
        handler_type: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        """Mark a message as successfully processed."""
        _mark_message_processed(self._get_connection(), message_id, handler_type, execution_id)

    def cleanup_old_processed_messages(self, max_age_hours: float = 24.0) -> int:
        """Clean up old processed message records."""
        return _cleanup_old_processed_messages(self._get_connection(), max_age_hours)

    def is_healthy(self) -> bool:
        """Check if the database connection is healthy."""
        try:
            conn = self._get_connection()
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    @contextmanager
    def transaction(self, queue: Queue | None = None) -> Iterator[StoreTransaction]:
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
        from stabilize.persistence.sqlite.transaction import AtomicTransaction

        conn = self._get_connection()
        txn = AtomicTransaction(conn, self)
        try:
            yield txn
            conn.commit()
        except Exception:
            conn.rollback()
            # Restore in-memory versions to match rolled-back database state
            txn.rollback_versions()
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
            workflows.append(row_to_execution(row))
        return workflows
