"""Helper functions for SQLite persistence."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from stabilize.errors import ConcurrencyError

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution


def insert_stage(conn: sqlite3.Connection, stage: StageExecution, execution_id: str) -> None:
    """Insert a stage."""
    conn.execute(
        """
        INSERT INTO stage_executions (
            id, execution_id, ref_id, type, name, status, context, outputs,
            requisite_stage_ref_ids, parent_stage_id, synthetic_stage_owner,
            start_time, end_time, start_time_expiry, scheduled_time, version,
            join_type, join_threshold, split_type, split_conditions,
            mi_config, deferred_choice_group, milestone_ref_id,
            milestone_status, mutex_key, cancel_region
        ) VALUES (
            :id, :execution_id, :ref_id, :type, :name, :status,
            :context, :outputs, :requisite_stage_ref_ids,
            :parent_stage_id, :synthetic_stage_owner, :start_time,
            :end_time, :start_time_expiry, :scheduled_time, :version,
            :join_type, :join_threshold, :split_type, :split_conditions,
            :mi_config, :deferred_choice_group, :milestone_ref_id,
            :milestone_status, :mutex_key, :cancel_region
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
            "synthetic_stage_owner": (
                stage.synthetic_stage_owner.value if stage.synthetic_stage_owner else None
            ),
            "start_time": stage.start_time,
            "end_time": stage.end_time,
            "start_time_expiry": stage.start_time_expiry,
            "scheduled_time": stage.scheduled_time,
            "version": stage.version,
            "join_type": stage.join_type.value,
            "join_threshold": stage.join_threshold,
            "split_type": stage.split_type.value,
            "split_conditions": json.dumps(stage.split_conditions),
            "mi_config": json.dumps(stage.mi_config.to_dict()) if stage.mi_config else None,
            "deferred_choice_group": stage.deferred_choice_group,
            "milestone_ref_id": stage.milestone_ref_id,
            "milestone_status": stage.milestone_status,
            "mutex_key": stage.mutex_key,
            "cancel_region": stage.cancel_region,
        },
    )

    # Insert tasks
    for task in stage.tasks:
        upsert_task(conn, task, stage.id)


def upsert_task(conn: sqlite3.Connection, task: TaskExecution, stage_id: str) -> None:
    """Insert or update a task with optimistic locking."""
    # First try to update existing row with version check
    cursor = conn.execute(
        """
        UPDATE task_executions SET
            name = :name,
            implementing_class = :implementing_class,
            status = :status,
            start_time = :start_time,
            end_time = :end_time,
            stage_start = :stage_start,
            stage_end = :stage_end,
            loop_start = :loop_start,
            loop_end = :loop_end,
            task_exception_details = :task_exception_details,
            version = version + 1
        WHERE id = :id AND version = :version
        """,
        {
            "id": task.id,
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
            "version": task.version,
        },
    )

    if cursor.rowcount == 0:
        # Row doesn't exist or version mismatch - try insert
        try:
            conn.execute(
                """
                INSERT INTO task_executions (
                    id, stage_id, name, implementing_class, status,
                    start_time, end_time, stage_start, stage_end,
                    loop_start, loop_end, task_exception_details, version
                ) VALUES (
                    :id, :stage_id, :name, :implementing_class, :status,
                    :start_time, :end_time, :stage_start, :stage_end,
                    :loop_start, :loop_end, :task_exception_details, 0
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
        except sqlite3.IntegrityError:
            # Row exists but version mismatch - concurrent modification
            raise ConcurrencyError(
                f"Task {task.id} was modified concurrently (expected version {task.version})"
            )
    else:
        # Update successful - increment local version
        task.version += 1
