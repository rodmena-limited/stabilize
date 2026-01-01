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
        """Fetch workflows with their stages and tasks."""
        workflows: list[WorkflowView] = []

        # Use existing store methods to fetch workflows
        # We'll need to adapt based on the available methods
        try:
            if app_filter:
                raw_workflows = list(self.store.retrieve_by_application(app_filter))
            else:
                # Get recent workflows - we need to use the connection directly
                raw_workflows = self._fetch_recent_workflows(limit, status_filter)

            for wf in raw_workflows[:limit]:
                workflow_view = self._convert_workflow(wf)
                workflows.append(workflow_view)

        except Exception:
            # If retrieve_by_application doesn't exist, try alternative
            raw_workflows = self._fetch_recent_workflows(limit, status_filter)
            for wf in raw_workflows[:limit]:
                workflow_view = self._convert_workflow(wf)
                workflows.append(workflow_view)

        # Sort: running first, then by start time ascending (oldest first, newest at bottom)
        workflows.sort(
            key=lambda w: (
                0 if w.status == WorkflowStatus.RUNNING else 1,
                w.start_time or 0,
            )
        )

        return workflows

    def _fetch_recent_workflows(
        self,
        limit: int,
        status_filter: str,
    ) -> list:
        """Fetch recent workflows directly from the database."""
        from stabilize.models.workflow import Workflow

        workflows: list[Workflow] = []

        # Access the connection manager from the store
        if hasattr(self.store, "_manager") and hasattr(self.store, "connection_string"):
            conn_mgr = self.store._manager  # type: ignore[attr-defined]
            conn = conn_mgr.get_sqlite_connection(self.store.connection_string)  # type: ignore[attr-defined]
            try:
                cursor = conn.cursor()

                # Build query based on status filter
                if status_filter == "running":
                    status_clause = "WHERE status = 'RUNNING'"
                elif status_filter == "failed":
                    status_clause = "WHERE status IN ('TERMINAL', 'FAILED_CONTINUE', 'CANCELED')"
                elif status_filter == "recent":
                    status_clause = ""  # All statuses, just recent
                else:
                    status_clause = ""

                query = f"""
                    SELECT id FROM pipeline_executions
                    {status_clause}
                    ORDER BY
                        CASE WHEN status = 'RUNNING' THEN 0 ELSE 1 END,
                        created_at DESC
                    LIMIT ?
                """
                cursor.execute(query, (limit,))
                rows = cursor.fetchall()

                for row in rows:
                    try:
                        wf = self.store.retrieve(row[0])
                        workflows.append(wf)
                    except Exception:
                        pass
            finally:
                pass  # Connection is managed by the connection manager

        return workflows

    def _convert_workflow(self, wf: Any) -> WorkflowView:
        """Convert a Workflow model to WorkflowView."""
        stages = []
        for stage in wf.stages:
            tasks = []
            for task in stage.tasks:
                error = None
                if task.task_exception_details:
                    error = task.task_exception_details.get("message", str(task.task_exception_details))

                tasks.append(
                    TaskView(
                        name=task.name,
                        implementing_class=task.implementing_class,
                        status=task.status,
                        start_time=task.start_time,
                        end_time=task.end_time,
                        error=error,
                    )
                )

            stages.append(
                StageView(
                    ref_id=stage.ref_id,
                    name=stage.name,
                    status=stage.status,
                    start_time=stage.start_time,
                    end_time=stage.end_time,
                    tasks=tasks,
                )
            )

        return WorkflowView(
            id=wf.id,
            application=wf.application,
            name=wf.name,
            status=wf.status,
            start_time=wf.start_time,
            end_time=wf.end_time,
            stages=stages,
        )

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
