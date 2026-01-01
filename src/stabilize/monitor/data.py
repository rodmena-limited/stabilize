from __future__ import annotations
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any
from stabilize.models.status import WorkflowStatus

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

    def stage_progress(self) -> tuple[int, int]:
        """Return (completed, total) stage counts."""
        total = len(self.stages)
        completed = sum(1 for s in self.stages if s.status.is_complete)
        return completed, total

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
    stuck: int = 0

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
