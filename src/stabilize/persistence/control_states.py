"""Workflow statuses from which a workflow may be paused."""

from __future__ import annotations

from stabilize.models.status import WorkflowStatus

PAUSABLE = (WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED, WorkflowStatus.BUFFERED)
