"""
Stage operations mixin for SqliteWorkflowStore.

Provides store_stage, add_stage, remove_stage, and retrieve_stage operations.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from stabilize.errors import ConcurrencyError
from stabilize.persistence.sqlite.converters import (
    row_to_execution,
    row_to_stage,
    row_to_task,
)
from stabilize.persistence.sqlite.helpers import insert_stage, upsert_task

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.workflow import Workflow


class SqliteStageOpsMixin:
    """Mixin providing stage operations."""

    def _get_connection(self) -> sqlite3.Connection: ...

    def store_stage(
        self,
        stage: StageExecution,
        expected_phase: str | None = None,
    ) -> None:
        """Store or update a stage.

        Args:
            stage: The stage to store
            expected_phase: If provided, adds status check to WHERE clause for
                           phase-aware optimistic locking.
        """
        conn = self._get_connection()

        # Check if stage exists
        result = conn.execute(
            "SELECT id FROM stage_executions WHERE id = :id",
            {"id": stage.id},
        )
        exists = result.fetchone() is not None

        if exists:
            # Build update query with optimistic locking
            # Optionally include phase check
            if expected_phase is not None:
                cursor = conn.execute(
                    """
                    UPDATE stage_executions SET
                        status = :status,
                        context = :context,
                        outputs = :outputs,
                        start_time = :start_time,
                        end_time = :end_time,
                        version = version + 1
                    WHERE id = :id AND version = :version AND status = :expected_phase
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
                if expected_phase is not None:
                    raise ConcurrencyError(
                        f"Optimistic lock failed for stage {stage.id} "
                        f"(version {stage.version}, expected_phase {expected_phase})"
                    )
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

        # Get tasks (ORDER BY id ensures consistent task sequencing)
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

        return stage
