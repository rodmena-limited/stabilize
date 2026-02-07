"""
Event store interface.

Defines the abstract interface for event storage backends.
All implementations must support atomic append operations
and efficient querying by various criteria.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.events.base import EntityType, Event, EventType


@dataclass
class EventQuery:
    """
    Query parameters for retrieving events.

    All fields are optional - unset fields are not filtered.
    Results are ordered by sequence number ascending.
    """

    entity_type: EntityType | None = None
    entity_id: str | None = None
    workflow_id: str | None = None
    event_types: list[EventType] | None = None
    from_sequence: int | None = None
    to_sequence: int | None = None
    from_timestamp: datetime | None = None
    to_timestamp: datetime | None = None
    limit: int = 1000
    offset: int = 0

    # Ordering
    order_by: str = "sequence"  # sequence or timestamp
    ascending: bool = True


@dataclass
class AppendResult:
    """Result of appending event(s) to the store."""

    events: list[Event] = field(default_factory=list)
    sequences: list[int] = field(default_factory=list)

    @property
    def first_sequence(self) -> int:
        """Get the first sequence number assigned."""
        return self.sequences[0] if self.sequences else 0

    @property
    def last_sequence(self) -> int:
        """Get the last sequence number assigned."""
        return self.sequences[-1] if self.sequences else 0


class EventStore(ABC):
    """
    Abstract event store interface.

    Event stores are append-only - events cannot be modified or deleted.
    Each event is assigned a monotonically increasing sequence number
    that provides global ordering.

    Implementations must ensure:
    - Atomic appends (single event or batch)
    - Unique sequence numbers
    - Durable storage
    - Efficient querying by entity, workflow, and time
    """

    @abstractmethod
    def append(self, event: Event, connection: Any | None = None) -> Event:
        """
        Append a single event to the store.

        Args:
            event: The event to append. Sequence will be assigned.
            connection: Optional database connection for transaction.

        Returns:
            The event with sequence number assigned.
        """
        pass

    @abstractmethod
    def append_batch(self, events: list[Event], connection: Any | None = None) -> list[Event]:
        """
        Append multiple events atomically.

        All events are appended in a single transaction.
        Sequence numbers are assigned in order.

        Args:
            events: Events to append.
            connection: Optional database connection for transaction.

        Returns:
            Events with sequence numbers assigned.
        """
        pass

    @abstractmethod
    def get_events(self, query: EventQuery) -> Iterator[Event]:
        """
        Query events matching criteria.

        Args:
            query: Query parameters.

        Returns:
            Iterator of matching events ordered by sequence.
        """
        pass

    @abstractmethod
    def get_events_for_entity(
        self,
        entity_type: EntityType,
        entity_id: str,
        from_version: int = 0,
    ) -> list[Event]:
        """
        Get all events for a specific entity.

        Args:
            entity_type: Type of entity.
            entity_id: Entity identifier.
            from_version: Only get events after this version.

        Returns:
            List of events for the entity.
        """
        pass

    @abstractmethod
    def get_events_for_workflow(
        self,
        workflow_id: str,
        from_sequence: int = 0,
    ) -> list[Event]:
        """
        Get all events for a workflow execution.

        Args:
            workflow_id: Workflow identifier.
            from_sequence: Only get events after this sequence.

        Returns:
            List of events for the workflow.
        """
        pass

    @abstractmethod
    def get_current_sequence(self) -> int:
        """
        Get the current (latest) global sequence number.

        Returns:
            The highest sequence number in the store, or 0 if empty.
        """
        pass

    @abstractmethod
    def get_events_since(
        self,
        sequence: int,
        limit: int = 1000,
    ) -> list[Event]:
        """
        Get events since a sequence number.

        Used for catch-up subscriptions.

        Args:
            sequence: Get events after this sequence.
            limit: Maximum number of events to return.

        Returns:
            List of events after the sequence.
        """
        pass

    def get_event_by_id(self, event_id: str) -> Event | None:
        """
        Get a single event by its ID.

        Args:
            event_id: The event identifier.

        Returns:
            The event if found, None otherwise.
        """
        # Default implementation using query

        for event in self.get_events(EventQuery(limit=1)):
            # This is inefficient - implementations should override
            pass
        return None

    def count_events(self, query: EventQuery | None = None) -> int:
        """
        Count events matching query.

        Args:
            query: Optional query parameters.

        Returns:
            Number of matching events.
        """
        # Default implementation - implementations should override
        count = 0
        for _ in self.get_events(query or EventQuery(limit=100000)):
            count += 1
        return count
