"""Data fetching layer for the monitor command."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from stabilize.models.status import WorkflowStatus

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue


@dataclass
class TaskView:
    """Lightweight view of a task for monitoring."""

    name: str
    implementing_class: str
    status: WorkflowStatus
    start_time: int | None
    end_time: int | None
    error: str | None = None


@dataclass
class StageView:
    """Lightweight view of a stage for monitoring."""

    ref_id: str
    name: str
    status: WorkflowStatus
    start_time: int | None
    end_time: int | None
    tasks: list[TaskView] = field(default_factory=list)


@dataclass
class WorkflowView:
    """Lightweight view of a workflow for monitoring."""

    id: str
    application: str
    name: str
    status: WorkflowStatus
    start_time: int | None
    end_time: int | None
    stages: list[StageView] = field(default_factory=list)

    @property
    def stage_progress(self) -> tuple[int, int]:
        """Return (completed, total) stage counts."""
        total = len(self.stages)
        completed = sum(1 for s in self.stages if s.status.is_complete)
        return completed, total

    @property
    def duration_ms(self) -> int | None:
        """Calculate duration in milliseconds."""
        if self.start_time is None:
            return None
        end = self.end_time or int(time.time() * 1000)
        return end - self.start_time


@dataclass
class QueueStats:
    """Queue statistics for monitoring."""

    pending: int = 0
    processing: int = 0
    stuck: int = 0  # locked_until > now + threshold


@dataclass
class WorkflowStats:
    """Aggregate workflow statistics."""

    running: int = 0
    succeeded: int = 0
    failed: int = 0
    total: int = 0


@dataclass
class MonitorData:
    """Complete data snapshot for the monitor display."""

    workflows: list[WorkflowView]
    queue_stats: QueueStats
    workflow_stats: WorkflowStats
    fetch_time: datetime
    error: str | None = None


def format_duration(start_ms: int | None, end_ms: int | None = None) -> str:
    """Format duration from milliseconds to human-readable string."""
    if start_ms is None:
        return "-"
    end = end_ms or int(time.time() * 1000)
    seconds = (end - start_ms) // 1000
    if seconds < 0:
        return "-"
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"


class MonitorDataFetcher:
    """Fetches monitoring data from the database."""

    def __init__(
        self,
        store: WorkflowStore,
        queue: Queue | None = None,
        stuck_threshold_seconds: int = 300,
    ):
        self.store = store
        self.queue = queue
        self.stuck_threshold_seconds = stuck_threshold_seconds

    def fetch(
        self,
        app_filter: str | None = None,
        limit: int = 50,
        status_filter: str = "all",
    ) -> MonitorData:
        """Fetch current monitoring data from the database."""
        try:
            workflows = self._fetch_workflows(app_filter, limit, status_filter)
            queue_stats = self._fetch_queue_stats()
            workflow_stats = self._calculate_workflow_stats(workflows)

            return MonitorData(
                workflows=workflows,
                queue_stats=queue_stats,
                workflow_stats=workflow_stats,
                fetch_time=datetime.now(),
            )
        except Exception as e:
            return MonitorData(
                workflows=[],
                queue_stats=QueueStats(),
                workflow_stats=WorkflowStats(),
                fetch_time=datetime.now(),
                error=str(e),
            )

    def _fetch_workflows(
        self,
        app_filter: str | None,
        limit: int,
        status_filter: str,
    ) -> list[WorkflowView]:
        """Fetch workflows efficiently using batch queries (3-query pattern)."""
        workflows_map: dict[str, WorkflowView] = {}
        stage_ids: list[str] = []
        workflow_ids: list[str] = []

        # 1. Fetch Workflows
        # ------------------
        if hasattr(self.store, "_pool"):
            # PostgreSQL
            with self.store._pool.connection() as conn:  # type: ignore
                with conn.cursor() as cur:
                    # Build Query
                    query, params = self._build_workflow_query(app_filter, limit, status_filter, dialect="postgres")
                    cur.execute(query, params)
                    rows = cur.fetchall()

                    for row in rows:
                        wf_view = self._row_to_workflow_view(row)
                        workflows_map[wf_view.id] = wf_view
                        workflow_ids.append(wf_view.id)

                    if not workflow_ids:
                        return []

                    # 2. Fetch Stages
                    # ---------------
                    cur.execute(
                        """
                        SELECT * FROM stage_executions
                        WHERE execution_id = ANY(%(ids)s)
                        ORDER BY start_time ASC
                        """,
                        {"ids": workflow_ids},
                    )
                    stage_rows = cur.fetchall()

                    stage_map = {}  # id -> StageView
                    for row in stage_rows:
                        stage_view = self._row_to_stage_view(row)
                        # Temp map by ref_id for linking? No, need unique ID
                        stage_map[stage_view.ref_id] = stage_view
                        # Actually we need to attach to workflow immediately
                        if stage_view.execution_id in workflows_map:  # type: ignore
                            workflows_map[stage_view.execution_id].stages.append(stage_view)  # type: ignore
                            stage_ids.append(row["id"])  # type: ignore

                    if not stage_ids:
                        return list(workflows_map.values())

                    # 3. Fetch Tasks
                    # --------------
                    cur.execute(
                        """
                        SELECT * FROM task_executions
                        WHERE stage_id = ANY(%(ids)s)
                        ORDER BY start_time ASC
                        """,
                        {"ids": stage_ids},
                    )
                    task_rows = cur.fetchall()

                    # Map tasks to stages. We need a way to find stage by ID.
                    # Re-iterate stages to build a lookup map by DB ID
                    stage_lookup = {}
                    for wf in workflows_map.values():
                        for stage in wf.stages:
                            # We need the internal DB ID to link tasks, but StageView doesn't have it by default.
                            # We stored it in _row_to_stage_view temporarily
                            if hasattr(stage, "_db_id"):
                                stage_lookup[stage._db_id] = stage  # type: ignore

                    for row in task_rows:
                        task_view = self._row_to_task_view(row)
                        stage_id = row["stage_id"]  # type: ignore
                        if stage_id in stage_lookup:
                            stage_lookup[stage_id].tasks.append(task_view)

        elif hasattr(self.store, "_manager"):
            # SQLite
            conn_mgr = self.store._manager  # type: ignore
            conn = conn_mgr.get_sqlite_connection(self.store.connection_string)  # type: ignore
            cursor = conn.cursor()

            # 1. Fetch Workflows
            query, params = self._build_workflow_query(app_filter, limit, status_filter, dialect="sqlite")
            cursor.execute(query, params)
            rows = cursor.fetchall()

            for row in rows:
                wf_view = self._row_to_workflow_view(row)
                workflows_map[wf_view.id] = wf_view
                workflow_ids.append(wf_view.id)

            if not workflow_ids:
                return []

            # 2. Fetch Stages
            # SQLite doesn't support = ANY(), use IN (?,?,?)
            placeholders = ",".join("?" * len(workflow_ids))
            cursor.execute(
                f"""
                SELECT * FROM stage_executions
                WHERE execution_id IN ({placeholders})
                ORDER BY start_time ASC
                """,
                workflow_ids,
            )
            stage_rows = cursor.fetchall()

            stage_lookup = {}  # db_id -> StageView

            for row in stage_rows:
                stage_view = self._row_to_stage_view(row)
                # SQLite rows can be accessed by name if RowFactory is set, usually is in Stabilize
                ex_id = row["execution_id"]
                if ex_id in workflows_map:
                    workflows_map[ex_id].stages.append(stage_view)
                    stage_ids.append(row["id"])
                    stage_lookup[row["id"]] = stage_view

            if not stage_ids:
                return list(workflows_map.values())

            # 3. Fetch Tasks
            placeholders = ",".join("?" * len(stage_ids))
            cursor.execute(
                f"""
                SELECT * FROM task_executions
                WHERE stage_id IN ({placeholders})
                ORDER BY start_time ASC
                """,
                stage_ids,
            )
            task_rows = cursor.fetchall()

            for row in task_rows:
                task_view = self._row_to_task_view(row)
                st_id = row["stage_id"]
                if st_id in stage_lookup:
                    stage_lookup[st_id].tasks.append(task_view)

        # Sort stages and tasks just in case DB didn't
        # And convert dictionary to list
        sorted_workflows = sorted(
            workflows_map.values(), key=lambda w: (0 if w.status == WorkflowStatus.RUNNING else 1, w.start_time or 0)
        )
        return sorted_workflows

    def _build_workflow_query(self, app: str | None, limit: int, status: str, dialect: str) -> tuple[str, Any]:
        """Build the SQL query for fetching workflows."""
        clauses = []
        params = {} if dialect == "postgres" else []

        if app:
            clauses.append("application = %(app)s" if dialect == "postgres" else "application = ?")
            if dialect == "postgres":
                params["app"] = app
            else:
                params.append(app)  # type: ignore

        if status == "running":
            clauses.append("status = 'RUNNING'")
        elif status == "failed":
            clauses.append("status IN ('TERMINAL', 'FAILED_CONTINUE', 'CANCELED')")

        where = "WHERE " + " AND ".join(clauses) if clauses else ""

        limit_param = "%(limit)s" if dialect == "postgres" else "?"
        if dialect == "postgres":
            params["limit"] = limit
        else:
            params.append(limit)  # type: ignore

        query = f"""
            SELECT * FROM pipeline_executions
            {where}
            ORDER BY
                CASE WHEN status = 'RUNNING' THEN 0 ELSE 1 END,
                created_at DESC
            LIMIT {limit_param}
        """
        return query, params

    def _row_to_workflow_view(self, row: Any) -> WorkflowView:
        return WorkflowView(
            id=row["id"],
            application=row["application"],
            name=row["name"] or "",
            status=WorkflowStatus[row["status"]],
            start_time=row["start_time"],
            end_time=row["end_time"],
            stages=[],
        )

    def _row_to_stage_view(self, row: Any) -> StageView:
        view = StageView(
            ref_id=row["ref_id"],
            name=row["name"] or "",
            status=WorkflowStatus[row["status"]],
            start_time=row["start_time"],
            end_time=row["end_time"],
            tasks=[],
        )
        # Attach internal DB info for linking
        view._db_id = row["id"]  # type: ignore
        view.execution_id = row["execution_id"]  # type: ignore
        return view

    def _row_to_task_view(self, row: Any) -> TaskView:
        # Handle JSON exception details safely
        error = None
        try:
            details = row["task_exception_details"]
            if details:
                if isinstance(details, str):
                    import json

                    details = json.loads(details)
                if isinstance(details, dict):
                    error = details.get("message")
        except Exception:
            pass

        return TaskView(
            name=row["name"],
            implementing_class=row["implementing_class"],
            status=WorkflowStatus[row["status"]],
            start_time=row["start_time"],
            end_time=row["end_time"],
            error=error,
        )

        # Removed _fetch_recent_workflows and _convert_workflow as they are replaced by the batch logic above

    def _fetch_queue_stats(self) -> QueueStats:
        """Fetch queue statistics."""
        if self.queue is None:
            return QueueStats()

        try:
            # Try to get queue stats from the queue object
            if hasattr(self.queue, "_manager") and hasattr(self.queue, "connection_string"):
                conn_mgr = self.queue._manager  # type: ignore[attr-defined]
                conn = conn_mgr.get_sqlite_connection(self.queue.connection_string)  # type: ignore[attr-defined]
                cursor = conn.cursor()

                now = datetime.now().isoformat()
                threshold = int(time.time()) - self.stuck_threshold_seconds

                # Count pending (not locked, ready to deliver)
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM queue_messages
                    WHERE (locked_until IS NULL OR locked_until < ?)
                    AND deliver_at <= ?
                    """,
                    (now, now),
                )
                pending = cursor.fetchone()[0]

                # Count processing (locked and not expired)
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM queue_messages
                    WHERE locked_until IS NOT NULL AND locked_until > ?
                    """,
                    (now,),
                )
                processing = cursor.fetchone()[0]

                # Count stuck (locked for too long)
                stuck_threshold = datetime.fromtimestamp(threshold).isoformat()
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM queue_messages
                    WHERE locked_until IS NOT NULL
                    AND locked_until < ?
                    AND locked_until < ?
                    """,
                    (now, stuck_threshold),
                )
                stuck = cursor.fetchone()[0]

                return QueueStats(pending=pending, processing=processing, stuck=stuck)

        except Exception:
            pass

        return QueueStats()

    def _calculate_workflow_stats(self, workflows: list[WorkflowView]) -> WorkflowStats:
        """Calculate aggregate workflow statistics."""
        stats = WorkflowStats(total=len(workflows))

        for wf in workflows:
            if wf.status == WorkflowStatus.RUNNING:
                stats.running += 1
            elif wf.status == WorkflowStatus.SUCCEEDED:
                stats.succeeded += 1
            elif wf.status.is_failure:
                stats.failed += 1

        return stats
