"""
Event replay for state reconstruction.

The EventReplayer allows rebuilding workflow state at any point
in time by replaying events from the event store.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from stabilize.events.base import EntityType, Event, EventType

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from stabilize.events.snapshots import SnapshotStore
    from stabilize.events.store.interface import EventStore


@dataclass
class ReplayResult:
    """Result of replaying events for an entity."""

    entity_type: EntityType
    entity_id: str
    final_state: dict[str, Any] = field(default_factory=dict)
    events_replayed: int = 0
    from_snapshot: bool = False
    snapshot_sequence: int = 0


@dataclass
class WorkflowState:
    """Reconstructed workflow state from events."""

    workflow_id: str
    status: str | None = None
    application: str | None = None
    name: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    context: dict[str, Any] = field(default_factory=dict)
    stages: dict[str, dict[str, Any]] = field(default_factory=dict)  # stage_id -> state
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)  # task_id -> state

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "workflow_id": self.workflow_id,
            "status": self.status,
            "application": self.application,
            "name": self.name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "context": self.context,
            "stages": self.stages,
            "tasks": self.tasks,
        }


class EventReplayer:
    """
    Replays events to reconstruct state.

    Supports:
    - Full state reconstruction from events
    - Time-travel queries (state at a specific point)
    - Incremental replay from snapshots
    - Custom event handlers for testing/debugging
    """

    def __init__(
        self,
        event_store: EventStore,
        snapshot_store: SnapshotStore | None = None,
    ) -> None:
        """
        Initialize the event replayer.

        Args:
            event_store: The event store to replay from.
            snapshot_store: Optional snapshot store for faster replay.
        """
        self._event_store = event_store
        self._snapshot_store = snapshot_store

    def rebuild_workflow_state(
        self,
        workflow_id: str,
        as_of_sequence: int | None = None,
    ) -> dict[str, Any]:
        """
        Rebuild complete workflow state from events.

        Args:
            workflow_id: The workflow to rebuild.
            as_of_sequence: Rebuild state as of this sequence (for time travel).

        Returns:
            Dictionary containing the complete workflow state.
        """
        state = WorkflowState(workflow_id=workflow_id)
        start_sequence = 0

        # Try to start from snapshot
        if self._snapshot_store:
            snapshot = self._snapshot_store.get_latest_snapshot(EntityType.WORKFLOW, workflow_id)
            if snapshot and (as_of_sequence is None or snapshot.sequence <= as_of_sequence):
                # Load state from snapshot
                state = self._load_state_from_snapshot(snapshot)
                start_sequence = snapshot.sequence

        # Replay events since snapshot
        if as_of_sequence is not None:
            # Get events up to the specified sequence
            events = [
                e
                for e in self._event_store.get_events_for_workflow(workflow_id, start_sequence)
                if e.sequence <= as_of_sequence
            ]
        else:
            events = self._event_store.get_events_for_workflow(workflow_id, start_sequence)

        # Check for sequence gaps
        last_seq = 0
        for event in events:
            if last_seq > 0 and event.sequence > last_seq + 1:
                logger.warning(
                    "Sequence gap detected in workflow %s: %d -> %d (missing %d events)",
                    workflow_id,
                    last_seq,
                    event.sequence,
                    event.sequence - last_seq - 1,
                )
            self._apply_event(state, event)
            last_seq = event.sequence

        return state.to_dict()

    def time_travel_query(
        self,
        workflow_id: str,
        as_of_time: datetime,
    ) -> dict[str, Any]:
        """
        Get workflow state at a specific point in time.

        Args:
            workflow_id: The workflow to query.
            as_of_time: Get state as of this timestamp.

        Returns:
            Dictionary containing the workflow state at that time.
        """
        from stabilize.events.store.interface import EventQuery

        state = WorkflowState(workflow_id=workflow_id)

        # Query events up to the specified time
        query = EventQuery(
            workflow_id=workflow_id,
            to_timestamp=as_of_time,
            limit=100000,  # Get all events up to that time
        )

        for event in self._event_store.get_events(query):
            self._apply_event(state, event)

        return state.to_dict()

    def replay_workflow_from_checkpoint(
        self,
        workflow_id: str,
        checkpoint_sequence: int,
        event_handler: Callable[[Event], None],
    ) -> None:
        """
        Replay events through a custom handler.

        Useful for:
        - Testing event handlers
        - Debugging workflow execution
        - Building custom projections

        Args:
            workflow_id: The workflow to replay.
            checkpoint_sequence: Start replaying from this sequence.
            event_handler: Handler to call for each event.
        """
        events = self._event_store.get_events_for_workflow(workflow_id, checkpoint_sequence)

        for event in events:
            event_handler(event)

    def _load_state_from_snapshot(
        self,
        snapshot: Any,
    ) -> WorkflowState:
        """Load WorkflowState from a snapshot."""
        state_dict = snapshot.state
        return WorkflowState(
            workflow_id=snapshot.entity_id,
            status=state_dict.get("status"),
            application=state_dict.get("application"),
            name=state_dict.get("name"),
            context=state_dict.get("context", {}),
            stages=state_dict.get("stages", {}),
            tasks=state_dict.get("tasks", {}),
        )

    def _apply_event(self, state: WorkflowState, event: Event) -> None:
        """Apply an event to update the state."""
        if event.entity_type == EntityType.WORKFLOW:
            self._apply_workflow_event(state, event)
        elif event.entity_type == EntityType.STAGE:
            self._apply_stage_event(state, event)
        elif event.entity_type == EntityType.TASK:
            self._apply_task_event(state, event)

    def _apply_workflow_event(self, state: WorkflowState, event: Event) -> None:
        """Apply a workflow event."""
        if event.event_type == EventType.WORKFLOW_CREATED:
            state.application = event.data.get("application")
            state.name = event.data.get("name")

        elif event.event_type == EventType.WORKFLOW_STARTED:
            state.start_time = event.timestamp
            state.status = "RUNNING"
            if "context" in event.data:
                state.context.update(event.data["context"])

        elif event.event_type == EventType.WORKFLOW_COMPLETED:
            state.end_time = event.timestamp
            state.status = event.data.get("status", "SUCCEEDED")

        elif event.event_type == EventType.WORKFLOW_FAILED:
            state.end_time = event.timestamp
            state.status = event.data.get("status", "TERMINAL")

        elif event.event_type == EventType.WORKFLOW_CANCELED:
            state.end_time = event.timestamp
            state.status = "CANCELED"

        elif event.event_type == EventType.WORKFLOW_PAUSED:
            state.status = "PAUSED"

        elif event.event_type == EventType.WORKFLOW_RESUMED:
            state.status = "RUNNING"

        elif event.event_type == EventType.CONTEXT_UPDATED:
            if "context" in event.data:
                state.context.update(event.data["context"])

    def _apply_stage_event(self, state: WorkflowState, event: Event) -> None:
        """Apply a stage event."""
        stage_id = event.entity_id

        # Ensure stage entry exists
        if stage_id not in state.stages:
            state.stages[stage_id] = {
                "id": stage_id,
                "ref_id": event.data.get("ref_id"),
                "type": event.data.get("type"),
                "name": event.data.get("name"),
            }

        stage = state.stages[stage_id]

        if event.event_type == EventType.STAGE_STARTED:
            stage["status"] = "RUNNING"
            stage["start_time"] = event.timestamp.isoformat()

        elif event.event_type == EventType.STAGE_COMPLETED:
            stage["status"] = event.data.get("status", "SUCCEEDED")
            stage["end_time"] = event.timestamp.isoformat()
            if "outputs" in event.data:
                stage["outputs"] = event.data["outputs"]

        elif event.event_type == EventType.STAGE_FAILED:
            stage["status"] = event.data.get("status", "TERMINAL")
            stage["end_time"] = event.timestamp.isoformat()
            stage["error"] = event.data.get("error")

        elif event.event_type == EventType.STAGE_SKIPPED:
            stage["status"] = "SKIPPED"
            stage["skip_reason"] = event.data.get("reason")

        elif event.event_type == EventType.STAGE_CANCELED:
            stage["status"] = "CANCELED"

    def _apply_task_event(self, state: WorkflowState, event: Event) -> None:
        """Apply a task event."""
        task_id = event.entity_id

        # Ensure task entry exists
        if task_id not in state.tasks:
            state.tasks[task_id] = {
                "id": task_id,
                "name": event.data.get("name"),
                "stage_id": event.data.get("stage_id"),
            }

        task = state.tasks[task_id]

        if event.event_type == EventType.TASK_STARTED:
            task["status"] = "RUNNING"
            task["start_time"] = event.timestamp.isoformat()

        elif event.event_type == EventType.TASK_COMPLETED:
            task["status"] = event.data.get("status", "SUCCEEDED")
            task["end_time"] = event.timestamp.isoformat()
            if "outputs" in event.data:
                task["outputs"] = event.data["outputs"]

        elif event.event_type == EventType.TASK_FAILED:
            task["status"] = event.data.get("status", "TERMINAL")
            task["end_time"] = event.timestamp.isoformat()
            task["error"] = event.data.get("error")

        elif event.event_type == EventType.TASK_RETRIED:
            task["retry_count"] = event.data.get("retry_count", task.get("retry_count", 0) + 1)

    def get_entity_history(
        self,
        entity_type: EntityType,
        entity_id: str,
    ) -> list[dict[str, Any]]:
        """
        Get the complete event history for an entity.

        Args:
            entity_type: Type of entity.
            entity_id: Entity identifier.

        Returns:
            List of events for the entity.
        """
        events = self._event_store.get_events_for_entity(entity_type, entity_id)
        return [e.to_dict() for e in events]

    def compare_states(
        self,
        workflow_id: str,
        sequence1: int,
        sequence2: int,
    ) -> dict[str, Any]:
        """
        Compare workflow state at two different points.

        Useful for debugging what changed between two points in time.

        Args:
            workflow_id: The workflow to compare.
            sequence1: First sequence number.
            sequence2: Second sequence number.

        Returns:
            Dictionary showing differences between the two states.
        """
        state1 = self.rebuild_workflow_state(workflow_id, as_of_sequence=sequence1)
        state2 = self.rebuild_workflow_state(workflow_id, as_of_sequence=sequence2)

        return {
            "before": state1,
            "after": state2,
            "sequence_range": {"from": sequence1, "to": sequence2},
        }
