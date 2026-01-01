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
