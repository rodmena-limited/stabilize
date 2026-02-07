"""
Event sourcing base types.

This module defines the core event model used for event sourcing in Stabilize.
Events are immutable records of state changes that can be replayed to reconstruct
workflow state at any point in time.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


def _generate_event_id() -> str:
    """Generate a unique event ID using ULID."""
    from ulid import ULID

    return str(ULID())


def _utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(UTC)


class EventType(Enum):
    """
    All event types in the system.

    Events are organized by entity lifecycle:
    - workflow.* - Workflow-level events
    - stage.* - Stage-level events
    - task.* - Task-level events
    - status.* - Generic status changes
    - context.* - Context/output updates
    """

    # Workflow lifecycle
    WORKFLOW_CREATED = "workflow.created"
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_CANCELED = "workflow.canceled"
    WORKFLOW_PAUSED = "workflow.paused"
    WORKFLOW_RESUMED = "workflow.resumed"

    # Stage lifecycle
    STAGE_STARTED = "stage.started"
    STAGE_COMPLETED = "stage.completed"
    STAGE_FAILED = "stage.failed"
    STAGE_SKIPPED = "stage.skipped"
    STAGE_CANCELED = "stage.canceled"

    # Task lifecycle
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_RETRIED = "task.retried"

    # State changes
    STATUS_CHANGED = "status.changed"
    CONTEXT_UPDATED = "context.updated"
    OUTPUTS_UPDATED = "outputs.updated"
    JUMP_EXECUTED = "jump.executed"


class EntityType(Enum):
    """Entity types that can emit events."""

    WORKFLOW = "workflow"
    STAGE = "stage"
    TASK = "task"


@dataclass(frozen=True)
class EventMetadata:
    """
    Metadata attached to every event for tracing and debugging.

    Attributes:
        correlation_id: Links related events across a workflow execution.
                       Typically the workflow execution ID.
        causation_id: ID of the event that caused this event (for event chains).
        actor: Who or what triggered this event (user ID or "system").
        source_handler: The handler class that emitted this event.
    """

    correlation_id: str
    causation_id: str | None = None
    actor: str = "system"
    source_handler: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert metadata to dictionary for storage."""
        return {
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "actor": self.actor,
            "source_handler": self.source_handler,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EventMetadata:
        """Create metadata from dictionary."""
        return cls(
            correlation_id=data.get("correlation_id", ""),
            causation_id=data.get("causation_id"),
            actor=data.get("actor", "system"),
            source_handler=data.get("source_handler"),
        )


@dataclass(frozen=True)
class Event:
    """
    Immutable event record.

    Events are the source of truth for all state changes in the system.
    They are append-only and can be replayed to reconstruct state.

    Attributes:
        event_id: Unique identifier (ULID for time-ordering).
        event_type: Type of event from EventType enum.
        timestamp: When the event occurred (UTC).
        sequence: Global ordering number (assigned by EventStore on append).
        entity_type: Type of entity this event is about.
        entity_id: ID of the entity.
        workflow_id: Workflow ID for correlation (always set).
        version: Entity version after this event (for optimistic concurrency).
        data: Event-specific payload.
        metadata: Tracing and debugging metadata.
    """

    event_id: str = field(default_factory=_generate_event_id)
    event_type: EventType = EventType.STATUS_CHANGED
    timestamp: datetime = field(default_factory=_utc_now)
    sequence: int = 0  # Assigned by store on append
    entity_type: EntityType = EntityType.WORKFLOW
    entity_id: str = ""
    workflow_id: str = ""
    version: int = 0
    data: dict[str, Any] = field(default_factory=dict)
    metadata: EventMetadata = field(default_factory=lambda: EventMetadata(correlation_id=""))

    def with_sequence(self, sequence: int) -> Event:
        """Return a new event with the given sequence number."""
        return Event(
            event_id=self.event_id,
            event_type=self.event_type,
            timestamp=self.timestamp,
            sequence=sequence,
            entity_type=self.entity_type,
            entity_id=self.entity_id,
            workflow_id=self.workflow_id,
            version=self.version,
            data=self.data,
            metadata=self.metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary for storage."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "sequence": self.sequence,
            "entity_type": self.entity_type.value,
            "entity_id": self.entity_id,
            "workflow_id": self.workflow_id,
            "version": self.version,
            "data": self.data,
            "metadata": self.metadata.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        """Create event from dictionary."""
        timestamp = data.get("timestamp")
        if isinstance(timestamp, str):
            # Parse ISO format, handle both with and without timezone
            if timestamp.endswith("Z"):
                timestamp = timestamp[:-1] + "+00:00"
            timestamp = datetime.fromisoformat(timestamp)
        elif timestamp is None:
            timestamp = _utc_now()

        return cls(
            event_id=data.get("event_id", _generate_event_id()),
            event_type=EventType(data.get("event_type", "status.changed")),
            timestamp=timestamp,
            sequence=data.get("sequence", 0),
            entity_type=EntityType(data.get("entity_type", "workflow")),
            entity_id=data.get("entity_id", ""),
            workflow_id=data.get("workflow_id", ""),
            version=data.get("version", 0),
            data=data.get("data", {}),
            metadata=EventMetadata.from_dict(data.get("metadata", {})),
        )

    def __repr__(self) -> str:
        return (
            f"Event(id={self.event_id[:8]}..., "
            f"type={self.event_type.value}, "
            f"entity={self.entity_type.value}/{self.entity_id[:8]}..., "
            f"seq={self.sequence})"
        )


# Factory functions for common events


def create_workflow_event(
    event_type: EventType,
    workflow_id: str,
    version: int,
    data: dict[str, Any],
    metadata: EventMetadata,
) -> Event:
    """Create a workflow-level event."""
    return Event(
        event_type=event_type,
        entity_type=EntityType.WORKFLOW,
        entity_id=workflow_id,
        workflow_id=workflow_id,
        version=version,
        data=data,
        metadata=metadata,
    )


def create_stage_event(
    event_type: EventType,
    stage_id: str,
    workflow_id: str,
    version: int,
    data: dict[str, Any],
    metadata: EventMetadata,
) -> Event:
    """Create a stage-level event."""
    return Event(
        event_type=event_type,
        entity_type=EntityType.STAGE,
        entity_id=stage_id,
        workflow_id=workflow_id,
        version=version,
        data=data,
        metadata=metadata,
    )


def create_task_event(
    event_type: EventType,
    task_id: str,
    workflow_id: str,
    version: int,
    data: dict[str, Any],
    metadata: EventMetadata,
) -> Event:
    """Create a task-level event."""
    return Event(
        event_type=event_type,
        entity_type=EntityType.TASK,
        entity_id=task_id,
        workflow_id=workflow_id,
        version=version,
        data=data,
        metadata=metadata,
    )
