"""
Snapshot management for event sourcing.

Snapshots are periodic saves of entity state that allow
faster replay by starting from a checkpoint instead of
replaying all events from the beginning.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from stabilize.events.base import EntityType

if TYPE_CHECKING:
    from stabilize.events.store.interface import EventStore


@dataclass
class Snapshot:
    """
    A snapshot of entity state at a point in time.

    Snapshots capture:
    - Entity identity (type, id, workflow)
    - Version at snapshot time
    - Last event sequence included
    - Serialized state
    """

    entity_type: EntityType
    entity_id: str
    workflow_id: str
    version: int
    sequence: int  # Last event sequence included in this snapshot
    state: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @staticmethod
    def _compute_state_hash(state: dict[str, Any]) -> str:
        """Compute SHA-256 hash of state for integrity verification."""
        serialized = json.dumps(state, sort_keys=True, default=str).encode()
        return hashlib.sha256(serialized).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot to dictionary for storage."""
        return {
            "entity_type": self.entity_type.value,
            "entity_id": self.entity_id,
            "workflow_id": self.workflow_id,
            "version": self.version,
            "sequence": self.sequence,
            "state": self.state,
            "state_hash": self._compute_state_hash(self.state),
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Snapshot:
        """Create snapshot from dictionary."""
        created_at = data.get("created_at")
        if isinstance(created_at, str):
            if created_at.endswith("Z"):
                created_at = created_at[:-1] + "+00:00"
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now(UTC)

        state = data.get("state", {})

        # Verify state integrity if hash is present (backward compatible)
        stored_hash = data.get("state_hash")
        if stored_hash is not None:
            computed_hash = cls._compute_state_hash(state)
            if computed_hash != stored_hash:
                raise ValueError(
                    f"Snapshot state integrity check failed: "
                    f"expected {stored_hash[:16]}..., got {computed_hash[:16]}..."
                )

        return cls(
            entity_type=EntityType(data.get("entity_type", "workflow")),
            entity_id=data.get("entity_id", ""),
            workflow_id=data.get("workflow_id", ""),
            version=data.get("version", 0),
            sequence=data.get("sequence", 0),
            state=state,
            created_at=created_at,
        )


class SnapshotPolicy:
    """
    Determines when to create snapshots.

    Snapshots are created based on:
    - Number of events since last snapshot
    - Time since last snapshot
    - Entity importance (workflows more than tasks)
    """

    def __init__(
        self,
        event_interval: int = 100,
        time_interval_seconds: int = 3600,  # 1 hour
    ) -> None:
        """
        Initialize snapshot policy.

        Args:
            event_interval: Create snapshot after this many events.
            time_interval_seconds: Create snapshot after this many seconds.
        """
        self._event_interval = event_interval
        self._time_interval_seconds = time_interval_seconds
        self._events_since_snapshot: dict[str, int] = {}
        self._last_snapshot_time: dict[str, datetime] = {}

    def should_snapshot(
        self,
        entity_id: str,
        events_since: int,
    ) -> bool:
        """
        Check if an entity should be snapshotted.

        Args:
            entity_id: The entity identifier.
            events_since: Number of events since last snapshot.

        Returns:
            True if a snapshot should be created.
        """
        # Check event count threshold
        if events_since >= self._event_interval:
            return True

        # Check time threshold
        last_snapshot = self._last_snapshot_time.get(entity_id)
        if last_snapshot:
            now = datetime.now(UTC)
            elapsed = (now - last_snapshot).total_seconds()
            if elapsed >= self._time_interval_seconds:
                return True

        return False

    def record_snapshot(self, entity_id: str) -> None:
        """Record that a snapshot was created."""
        self._events_since_snapshot[entity_id] = 0
        self._last_snapshot_time[entity_id] = datetime.now(UTC)

    def record_event(self, entity_id: str) -> None:
        """Record that an event was added for an entity."""
        current = self._events_since_snapshot.get(entity_id, 0)
        self._events_since_snapshot[entity_id] = current + 1


class SnapshotStore:
    """
    Store for managing entity snapshots.

    Uses the event store's snapshot tables for persistence.
    """

    def __init__(
        self,
        event_store: EventStore,
        policy: SnapshotPolicy | None = None,
    ) -> None:
        """
        Initialize the snapshot store.

        Args:
            event_store: The event store for persistence.
            policy: Snapshot policy (defaults to every 100 events).
        """
        self._event_store = event_store
        self._policy = policy or SnapshotPolicy()

    def save_snapshot(self, snapshot: Snapshot) -> None:
        """
        Save a snapshot.

        Args:
            snapshot: The snapshot to save.
        """
        # Delegate to event store's snapshot methods
        if hasattr(self._event_store, "save_snapshot"):
            getattr(self._event_store, "save_snapshot")(
                entity_type=snapshot.entity_type,
                entity_id=snapshot.entity_id,
                workflow_id=snapshot.workflow_id,
                version=snapshot.version,
                sequence=snapshot.sequence,
                state=snapshot.state,
            )
            self._policy.record_snapshot(snapshot.entity_id)

    def get_latest_snapshot(
        self,
        entity_type: EntityType,
        entity_id: str,
    ) -> Snapshot | None:
        """
        Get the latest snapshot for an entity.

        Args:
            entity_type: Type of entity.
            entity_id: Entity identifier.

        Returns:
            The latest snapshot or None if not found.
        """
        if hasattr(self._event_store, "get_latest_snapshot"):
            data = getattr(self._event_store, "get_latest_snapshot")(entity_type, entity_id)
            if data:
                return Snapshot.from_dict(data)
        return None

    def create_workflow_snapshot(
        self,
        workflow_state: dict[str, Any],
        workflow_id: str,
        version: int,
        sequence: int,
    ) -> Snapshot:
        """
        Create a snapshot for a workflow.

        Args:
            workflow_state: The workflow state to snapshot.
            workflow_id: Workflow identifier.
            version: Current version.
            sequence: Last event sequence.

        Returns:
            The created snapshot.
        """
        snapshot = Snapshot(
            entity_type=EntityType.WORKFLOW,
            entity_id=workflow_id,
            workflow_id=workflow_id,
            version=version,
            sequence=sequence,
            state=workflow_state,
        )
        self.save_snapshot(snapshot)
        return snapshot

    def should_snapshot(
        self,
        entity_id: str,
        events_since_snapshot: int,
    ) -> bool:
        """Check if an entity should be snapshotted."""
        return self._policy.should_snapshot(entity_id, events_since_snapshot)

    def record_event(self, entity_id: str) -> None:
        """Record that an event was added."""
        self._policy.record_event(entity_id)
