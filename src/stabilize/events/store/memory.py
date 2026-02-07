"""
In-memory event store implementation.

Provides a simple, thread-safe event store for testing
and development. Events are not persisted to disk.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Any

from stabilize.events.base import EntityType, Event, EventType
from stabilize.events.store.interface import EventQuery, EventStore


class InMemoryEventStore(EventStore):
    """
    In-memory implementation of event store.

    Thread-safe using a lock for all operations.
    Useful for testing and development.

    Note: Events are lost when the process exits.
    """

    def __init__(self) -> None:
        """Initialize in-memory event store."""
        self._events: list[Event] = []
        self._events_by_id: dict[str, Event] = {}
        self._sequence = 0
        self._lock = threading.Lock()

        # Subscriptions
        self._subscriptions: dict[str, dict[str, Any]] = {}

        # Snapshots
        self._snapshots: dict[tuple[str, str], dict[str, Any]] = {}

    def append(self, event: Event, connection: Any | None = None) -> Event:
        """Append a single event to the store."""
        events = self.append_batch([event], connection)
        return events[0]

    def append_batch(self, events: list[Event], connection: Any | None = None) -> list[Event]:
        """Append multiple events atomically."""
        if not events:
            return []

        with self._lock:
            result_events = []

            for event in events:
                self._sequence += 1
                new_event = event.with_sequence(self._sequence)
                self._events.append(new_event)
                self._events_by_id[new_event.event_id] = new_event
                result_events.append(new_event)

            return result_events

    def get_events(self, query: EventQuery) -> Iterator[Event]:
        """Query events matching criteria."""
        with self._lock:
            events = list(self._events)

        filtered = self._filter_events(events, query)

        # Apply ordering
        reverse = not query.ascending
        if query.order_by == "timestamp":
            filtered.sort(key=lambda e: e.timestamp, reverse=reverse)
        else:
            filtered.sort(key=lambda e: e.sequence, reverse=reverse)

        # Apply pagination
        start = query.offset
        end = start + query.limit
        yield from filtered[start:end]

    def _filter_events(self, events: list[Event], query: EventQuery) -> list[Event]:
        """Filter events based on query criteria."""
        result = []

        for event in events:
            if query.entity_type is not None and event.entity_type != query.entity_type:
                continue

            if query.entity_id is not None and event.entity_id != query.entity_id:
                continue

            if query.workflow_id is not None and event.workflow_id != query.workflow_id:
                continue

            if query.event_types and event.event_type not in query.event_types:
                continue

            if query.from_sequence is not None and event.sequence <= query.from_sequence:
                continue

            if query.to_sequence is not None and event.sequence > query.to_sequence:
                continue

            if query.from_timestamp is not None and event.timestamp < query.from_timestamp:
                continue

            if query.to_timestamp is not None and event.timestamp > query.to_timestamp:
                continue

            result.append(event)

        return result

    def get_events_for_entity(
        self,
        entity_type: EntityType,
        entity_id: str,
        from_version: int = 0,
    ) -> list[Event]:
        """Get all events for a specific entity."""
        with self._lock:
            return [
                e
                for e in self._events
                if e.entity_type == entity_type and e.entity_id == entity_id and e.version > from_version
            ]

    def get_events_for_workflow(
        self,
        workflow_id: str,
        from_sequence: int = 0,
    ) -> list[Event]:
        """Get all events for a workflow execution."""
        with self._lock:
            return [e for e in self._events if e.workflow_id == workflow_id and e.sequence > from_sequence]

    def get_current_sequence(self) -> int:
        """Get the current (latest) global sequence number."""
        with self._lock:
            return self._sequence

    def get_events_since(
        self,
        sequence: int,
        limit: int = 1000,
    ) -> list[Event]:
        """Get events since a sequence number."""
        with self._lock:
            events = [e for e in self._events if e.sequence > sequence]
            return events[:limit]

    def get_event_by_id(self, event_id: str) -> Event | None:
        """Get a single event by its ID."""
        with self._lock:
            return self._events_by_id.get(event_id)

    def count_events(self, query: EventQuery | None = None) -> int:
        """Count events matching query."""
        if query is None:
            with self._lock:
                return len(self._events)

        count = 0
        for _ in self.get_events(query):
            count += 1
        return count

    def clear(self) -> None:
        """Clear all events (for testing)."""
        with self._lock:
            self._events.clear()
            self._events_by_id.clear()
            self._sequence = 0
            self._subscriptions.clear()
            self._snapshots.clear()

    # Snapshot methods

    def save_snapshot(
        self,
        entity_type: EntityType,
        entity_id: str,
        workflow_id: str,
        version: int,
        sequence: int,
        state: dict[str, Any],
    ) -> None:
        """Save a snapshot of entity state."""
        with self._lock:
            key = (entity_type.value, entity_id)
            self._snapshots[key] = {
                "entity_type": entity_type.value,
                "entity_id": entity_id,
                "workflow_id": workflow_id,
                "version": version,
                "sequence": sequence,
                "state": state,
            }

    def get_latest_snapshot(
        self,
        entity_type: EntityType,
        entity_id: str,
    ) -> dict[str, Any] | None:
        """Get the latest snapshot for an entity."""
        with self._lock:
            key = (entity_type.value, entity_id)
            return self._snapshots.get(key)

    # Subscription methods

    def save_subscription(
        self,
        subscription_id: str,
        event_types: list[EventType] | None,
        entity_filter: dict[str, Any] | None,
        last_sequence: int,
        webhook_url: str | None = None,
    ) -> None:
        """Save or update a durable subscription."""
        with self._lock:
            self._subscriptions[subscription_id] = {
                "id": subscription_id,
                "event_types": event_types,
                "entity_filter": entity_filter,
                "last_sequence": last_sequence,
                "webhook_url": webhook_url,
            }

    def get_subscription(self, subscription_id: str) -> dict[str, Any] | None:
        """Get a durable subscription by ID."""
        with self._lock:
            return self._subscriptions.get(subscription_id)

    def update_subscription_sequence(self, subscription_id: str, last_sequence: int) -> None:
        """Update the last processed sequence for a subscription."""
        with self._lock:
            if subscription_id in self._subscriptions:
                self._subscriptions[subscription_id]["last_sequence"] = last_sequence

    def delete_subscription(self, subscription_id: str) -> None:
        """Delete a durable subscription."""
        with self._lock:
            self._subscriptions.pop(subscription_id, None)

    def list_subscriptions(self) -> list[dict[str, Any]]:
        """List all durable subscriptions."""
        with self._lock:
            return [{"id": s["id"], "last_sequence": s["last_sequence"]} for s in self._subscriptions.values()]
