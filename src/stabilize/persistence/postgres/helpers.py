"""Helper functions for PostgreSQL persistence."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from stabilize.errors import ConcurrencyError

if TYPE_CHECKING:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution


def insert_stage(cur: Any, stage: StageExecution, execution_id: str) -> None:
    """Insert a stage."""
    cur.execute(
        """
        INSERT INTO stage_executions (
            id, execution_id, ref_id, type, name, status, context, outputs,
            requisite_stage_ref_ids, parent_stage_id, synthetic_stage_owner,
            start_time, end_time, start_time_expiry, scheduled_time, version,
            join_type, join_threshold, split_type, split_conditions,
            mi_config, deferred_choice_group, milestone_ref_id,
            milestone_status, mutex_key, cancel_region
        ) VALUES (
            %(id)s, %(execution_id)s, %(ref_id)s, %(type)s, %(name)s, %(status)s,
            %(context)s::jsonb, %(outputs)s::jsonb, %(requisite_stage_ref_ids)s,
            %(parent_stage_id)s, %(synthetic_stage_owner)s, %(start_time)s,
            %(end_time)s, %(start_time_expiry)s, %(scheduled_time)s, %(version)s,
            %(join_type)s, %(join_threshold)s, %(split_type)s,
            %(split_conditions)s::jsonb, %(mi_config)s::jsonb,
            %(deferred_choice_group)s, %(milestone_ref_id)s,
            %(milestone_status)s, %(mutex_key)s, %(cancel_region)s
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
            "requisite_stage_ref_ids": list(stage.requisite_stage_ref_ids),
            "parent_stage_id": stage.parent_stage_id,
            "synthetic_stage_owner": (stage.synthetic_stage_owner.value if stage.synthetic_stage_owner else None),
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

    # Batch insert tasks
    if stage.tasks:
        upsert_tasks_bulk(cur, stage.tasks, stage.id)


def upsert_task(cur: Any, task: TaskExecution, stage_id: str) -> None:
    """Insert or update a task."""
    cur.execute(
        """
        INSERT INTO task_executions (
            id, stage_id, name, implementing_class, status,
            start_time, end_time, stage_start, stage_end,
            loop_start, loop_end, task_exception_details
        ) VALUES (
            %(id)s, %(stage_id)s, %(name)s, %(implementing_class)s, %(status)s,
            %(start_time)s, %(end_time)s, %(stage_start)s, %(stage_end)s,
            %(loop_start)s, %(loop_end)s, %(task_exception_details)s::jsonb
        )
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            task_exception_details = EXCLUDED.task_exception_details
        """,
        {
            "id": task.id,
            "stage_id": stage_id,
            "name": task.name,
            "implementing_class": task.implementing_class,
            "status": task.status.name,
            "start_time": task.start_time,
            "end_time": task.end_time,
            "stage_start": task.stage_start,
            "stage_end": task.stage_end,
            "loop_start": task.loop_start,
            "loop_end": task.loop_end,
            "task_exception_details": json.dumps(task.task_exception_details),
        },
    )


def upsert_tasks_bulk(cur: Any, tasks: list[TaskExecution], stage_id: str) -> None:
    """
    Batch upsert tasks with optimistic locking.

    Uses INSERT ... ON CONFLICT to handle both new and existing tasks efficiently.
    For updates, it increments version and checks the previous version to prevent lost updates.

    Raises:
        ConcurrencyError: If any task update fails due to version mismatch.
    """
    if not tasks:
        return

    # Process each task individually to properly detect optimistic lock failures.
    for task in tasks:
        params = {
            "id": task.id,
            "stage_id": stage_id,
            "name": task.name,
            "implementing_class": task.implementing_class,
            "status": task.status.name,
            "start_time": task.start_time,
            "end_time": task.end_time,
            "stage_start": task.stage_start,
            "stage_end": task.stage_end,
            "loop_start": task.loop_start,
            "loop_end": task.loop_end,
            "task_exception_details": json.dumps(task.task_exception_details),
            "version": task.version,
        }

        # Use INSERT ... ON CONFLICT with RETURNING to detect what happened
        cur.execute(
            """
            INSERT INTO task_executions (
                id, stage_id, name, implementing_class, status,
                start_time, end_time, stage_start, stage_end,
                loop_start, loop_end, task_exception_details, version
            ) VALUES (
                %(id)s, %(stage_id)s, %(name)s, %(implementing_class)s, %(status)s,
                %(start_time)s, %(end_time)s, %(stage_start)s, %(stage_end)s,
                %(loop_start)s, %(loop_end)s, %(task_exception_details)s::jsonb, %(version)s
            )
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                task_exception_details = EXCLUDED.task_exception_details,
                version = task_executions.version + 1
            WHERE task_executions.version = EXCLUDED.version
            RETURNING version
            """,
            params,
        )

        result = cur.fetchone()
        if result:
            # Operation succeeded (insert or update), get the new version
            new_version = result[0] if isinstance(result, tuple) else result.get("version", task.version + 1)
            task.version = new_version
        else:
            # No row returned means the update failed due to version mismatch
            raise ConcurrencyError(
                f"Optimistic lock failed for task {task.id} (version {task.version}). "
                f"Another process has modified this task."
            )
