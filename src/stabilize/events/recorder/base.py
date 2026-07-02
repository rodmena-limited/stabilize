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
from stabilize.events.txn_scope import current_scope

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

        Note: Event recording is best-effort when called outside a DB transaction.
        If the process crashes between a workflow state update and event recording,
        the event log may be missing the most recent event. Replay-based state
        reconstruction should treat event logs as eventually consistent with the
        authoritative workflow state in the persistence store.

        Args:
            event: Event to record.
            connection: Optional DB connection for transaction. When provided,
                the event append participates in the caller's transaction,
                ensuring atomicity with workflow state changes.

        Returns:
            Event with sequence assigned.
        """
        scope = current_scope() if connection is None else None
        if scope is not None and connection is None and self._store_matches_scope(scope):
            # Join the caller's open store transaction: the event commits or
            # rolls back together with the state it describes.
            connection = scope.connection

        try:
            recorded = self._event_store.append(event, connection=connection)
        except Exception as e:
            logger.error(
                "Failed to persist event %s for %s/%s: %s",
                event.event_type.value if event.event_type else "unknown",
                event.entity_type.value if event.entity_type else "unknown",
                event.entity_id,
                e,
            )
            raise

        if self._publish_to_bus:
            if scope is not None:
                # Defer publication until the transaction commits; dropped on
                # rollback so subscribers never observe uncommitted state.
                scope.pending.append(recorded)
            else:
                try:
                    get_event_bus().publish(recorded)
                except Exception as e:
                    logger.warning("Failed to publish event to bus: %s", e)

        return recorded

    def _store_matches_scope(self, scope: Any) -> bool:
        """Whether this recorder's event store lives in the same database as
        the transaction bound to the current thread (safe to join)."""
        if scope.url is None:
            return False
        store_url = getattr(self._event_store, "_connection_string", None)
        return store_url is not None and store_url == scope.url

    def _record_batch(
        self,
        events: list[Event],
        connection: Any | None = None,
    ) -> list[Event]:
        """Record multiple events atomically."""
        if not events:
            return []

        scope = current_scope() if connection is None else None
        if scope is not None and connection is None and self._store_matches_scope(scope):
            connection = scope.connection

        recorded = self._event_store.append_batch(events, connection=connection)

        if self._publish_to_bus:
            if scope is not None:
                scope.pending.extend(recorded)
            else:
                try:
                    get_event_bus().publish_batch(recorded)
                except Exception as e:
                    logger.warning("Failed to publish events to bus: %s", e)

        return recorded
