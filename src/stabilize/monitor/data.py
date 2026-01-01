from __future__ import annotations
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any
from stabilize.models.status import WorkflowStatus

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
