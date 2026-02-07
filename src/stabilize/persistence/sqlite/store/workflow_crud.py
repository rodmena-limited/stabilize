"""
Workflow CRUD operations mixin for SqliteWorkflowStore.

Provides store, retrieve, update_status, delete, and exists operations.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from stabilize.persistence.sqlite.converters import (
    execution_to_dict,
    paused_to_dict,
    row_to_execution,
    row_to_stage,
    row_to_task,
)
from stabilize.persistence.sqlite.helpers import insert_stage
from stabilize.persistence.store import WorkflowNotFoundError

if TYPE_CHECKING:
    from stabilize.models.workflow import Workflow


class SqliteWorkflowCrudMixin:
    """Mixin providing workflow CRUD operations."""

    if TYPE_CHECKING:

        def _get_connection(self) -> sqlite3.Connection: ...

    def store(self, execution: Workflow) -> None:
        """Store a complete execution.

        Note: This will raise an error if the execution ID already exists.
        Use exists() to check first if needed.
        """
        conn = self._get_connection()

        # Insert execution
        conn.execute(
            """
            INSERT INTO pipeline_executions (
                id, type, application, name, status, context, start_time, end_time,
                start_time_expiry, trigger, is_canceled, canceled_by,
                cancellation_reason, paused, pipeline_config_id,
                is_limit_concurrent, max_concurrent_executions,
                keep_waiting_pipelines, origin
            ) VALUES (
                :id, :type, :application, :name, :status, :context, :start_time, :end_time,
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

            # Get tasks for stage (ORDER BY id ensures consistent task sequencing)
            # Note: id is a ULID that encodes creation time, preserving insertion order
            task_result = conn.execute(
                """
                SELECT * FROM task_executions
                WHERE stage_id = :stage_id
                ORDER BY id ASC
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
                "paused": (
                    json.dumps(paused_to_dict(execution.paused)) if execution.paused else None
                ),
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

    def exists(self, execution_id: str) -> bool:
        """Check if a workflow exists.

        Args:
            execution_id: The workflow ID to check

        Returns:
            True if the workflow exists, False otherwise
        """
        conn = self._get_connection()
        result = conn.execute(
            "SELECT 1 FROM pipeline_executions WHERE id = :id",
            {"id": execution_id},
        )
        return result.fetchone() is not None
