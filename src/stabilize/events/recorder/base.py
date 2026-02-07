"""
EventRecorderBase — core recording infrastructure.

Contains __init__, _record, and _record_batch methods.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

from stabilize.events.base import Event
from stabilize.events.bus import get_event_bus

if TYPE_CHECKING:
    from stabilize.events.store.interface import EventStore

logger = logging.getLogger(__name__)


class EventRecorderBase:
    """
    Base class for event recording infrastructure.

    Provides the core _record and _record_batch methods that integrate
    with EventStore for persistence and EventBus for pub/sub notifications.
    """

    def __init__(
        self,
        event_store: EventStore,
        publish_to_bus: bool = True,
    ) -> None:
        """
        Initialize the event recorder.

        Args:
            event_store: Store for persisting events.
            publish_to_bus: Whether to also publish events to the bus.
        """
        self._event_store = event_store
        self._publish_to_bus = publish_to_bus
        self._lock = threading.Lock()

    def _record(
        self,
        event: Event,
        connection: Any | None = None,
    ) -> Event:
        """
        Record an event to store and optionally publish to bus.

        Args:
            event: Event to record.
            connection: Optional DB connection for transaction.

        Returns:
            Event with sequence assigned.
        """
        recorded = self._event_store.append(event, connection=connection)

        if self._publish_to_bus:
            try:
                get_event_bus().publish(recorded)
            except Exception as e:
                logger.warning("Failed to publish event to bus: %s", e)

        return recorded

    def _record_batch(
        self,
        events: list[Event],
        connection: Any | None = None,
    ) -> list[Event]:
        """Record multiple events atomically."""
        if not events:
            return []

        recorded = self._event_store.append_batch(events, connection=connection)

        if self._publish_to_bus:
            try:
                get_event_bus().publish_batch(recorded)
            except Exception as e:
                logger.warning("Failed to publish events to bus: %s", e)

        return recorded
