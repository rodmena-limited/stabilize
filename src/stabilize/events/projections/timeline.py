"""
Workflow timeline projection.

Builds a human-readable timeline of workflow execution events,
showing stages, tasks, and their durations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from stabilize.events.base import EntityType, Event, EventType
from stabilize.events.projections.base import Projection


@dataclass
class TimelineEntry:
    """A single entry in the workflow timeline."""

    timestamp: datetime
    event_type: str
    entity_type: str
    entity_id: str
    entity_name: str | None = None
    status: str | None = None
    duration_ms: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "entity_name": self.entity_name,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "details": self.details,
        }


@dataclass
class WorkflowTimeline:
    """Complete timeline for a workflow execution."""

    workflow_id: str
    entries: list[TimelineEntry] = field(default_factory=list)
    total_duration_ms: int | None = None
    status: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "workflow_id": self.workflow_id,
            "entries": [e.to_dict() for e in self.entries],
            "total_duration_ms": self.total_duration_ms,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
        }


class WorkflowTimelineProjection(Projection):
    """
    Builds a human-readable timeline from workflow events.

    The timeline shows:
    - Workflow start/end with status
    - Stage start/end with duration
    - Task start/end with duration
    - Any failures or skips
    """

    def __init__(self, workflow_id: str) -> None:
        """
        Initialize the timeline projection.

        Args:
            workflow_id: The workflow ID to build timeline for.
        """
        self._workflow_id = workflow_id
        self._timeline = WorkflowTimeline(workflow_id=workflow_id)
        self._start_times: dict[str, datetime] = {}  # entity_id -> start_time

    @property
    def name(self) -> str:
        return f"workflow-timeline-{self._workflow_id}"

    def apply(self, event: Event) -> None:
        """Apply an event to the timeline."""
        # Only process events for this workflow
        if event.workflow_id != self._workflow_id:
            return

        entry = self._create_entry(event)
        if entry:
            self._timeline.entries.append(entry)

        # Update workflow-level state
        self._update_workflow_state(event)

    def _create_entry(self, event: Event) -> TimelineEntry | None:
        """Create a timeline entry from an event."""
        # Extract common fields
        entity_name = event.data.get("name") or event.data.get("ref_id")
        status = event.data.get("status")
        duration_ms = event.data.get("duration_ms")

        # Track start times for duration calculation
        if event.event_type in {
            EventType.WORKFLOW_STARTED,
            EventType.STAGE_STARTED,
            EventType.TASK_STARTED,
        }:
            self._start_times[event.entity_id] = event.timestamp

        # Calculate duration for completion events
        if event.event_type in {
            EventType.WORKFLOW_COMPLETED,
            EventType.WORKFLOW_FAILED,
            EventType.STAGE_COMPLETED,
            EventType.STAGE_FAILED,
            EventType.TASK_COMPLETED,
            EventType.TASK_FAILED,
        }:
            if duration_ms is None and event.entity_id in self._start_times:
                start = self._start_times[event.entity_id]
                delta = event.timestamp - start
                duration_ms = int(delta.total_seconds() * 1000)

        # Build details dict
        details = {}
        if event.event_type == EventType.WORKFLOW_CREATED:
            details = {
                "application": event.data.get("application"),
                "type": event.data.get("type"),
                "stage_count": event.data.get("stage_count"),
            }
        elif event.event_type == EventType.STAGE_STARTED:
            details = {
                "type": event.data.get("type"),
                "is_synthetic": event.data.get("is_synthetic"),
                "task_count": event.data.get("task_count"),
            }
        elif event.event_type in {EventType.STAGE_FAILED, EventType.TASK_FAILED}:
            details = {"error": event.data.get("error")}
        elif event.event_type == EventType.STAGE_SKIPPED:
            details = {"reason": event.data.get("reason")}

        # Skip internal events that don't need timeline entries
        if event.event_type in {
            EventType.STATUS_CHANGED,
            EventType.CONTEXT_UPDATED,
            EventType.OUTPUTS_UPDATED,
        }:
            return None

        return TimelineEntry(
            timestamp=event.timestamp,
            event_type=event.event_type.value,
            entity_type=event.entity_type.value,
            entity_id=event.entity_id,
            entity_name=entity_name,
            status=status,
            duration_ms=duration_ms,
            details=details,
        )

    def _update_workflow_state(self, event: Event) -> None:
        """Update workflow-level state from event."""
        if event.entity_type != EntityType.WORKFLOW:
            return

        if event.event_type == EventType.WORKFLOW_STARTED:
            self._timeline.start_time = event.timestamp

        elif event.event_type in {
            EventType.WORKFLOW_COMPLETED,
            EventType.WORKFLOW_FAILED,
            EventType.WORKFLOW_CANCELED,
        }:
            self._timeline.end_time = event.timestamp
            self._timeline.status = event.data.get("status")

            # Calculate total duration
            if self._timeline.start_time and self._timeline.end_time:
                delta = self._timeline.end_time - self._timeline.start_time
                self._timeline.total_duration_ms = int(delta.total_seconds() * 1000)

    def get_state(self) -> WorkflowTimeline:
        """Get the current timeline."""
        return self._timeline

    def reset(self) -> None:
        """Reset the projection."""
        self._timeline = WorkflowTimeline(workflow_id=self._workflow_id)
        self._start_times.clear()

    def get_stages(self) -> list[TimelineEntry]:
        """Get only stage-related entries."""
        return [e for e in self._timeline.entries if e.entity_type == "stage"]

    def get_tasks(self) -> list[TimelineEntry]:
        """Get only task-related entries."""
        return [e for e in self._timeline.entries if e.entity_type == "task"]

    def get_failures(self) -> list[TimelineEntry]:
        """Get only failure events."""
        return [
            e
            for e in self._timeline.entries
            if e.event_type in {"stage.failed", "task.failed", "workflow.failed", "workflow.canceled"}
        ]
