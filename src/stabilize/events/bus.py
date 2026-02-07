"""
In-process event bus for pub/sub.

Provides thread-safe event publishing and subscription
with support for filtering by event type and entity type.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Any

from stabilize.events.base import EntityType, Event, EventType

logger = logging.getLogger(__name__)


class SubscriptionMode(Enum):
    """How a subscription processes events."""

    SYNC = "sync"  # Block until processed
    ASYNC = "async"  # Fire and forget (thread pool)


@dataclass
class Subscription:
    """
    A subscription to events.

    Subscribers can filter by:
    - event_types: Only specific event types
    - entity_types: Only specific entity types
    - workflow_id: Only events for a specific workflow
    """

    id: str
    handler: Callable[[Event], Any]
    event_types: set[EventType] | None = None  # None = all
    entity_types: set[EntityType] | None = None  # None = all
    workflow_filter: str | None = None  # Specific workflow ID
    mode: SubscriptionMode = SubscriptionMode.SYNC
    enabled: bool = True

    def matches(self, event: Event) -> bool:
        """Check if this subscription should receive the event."""
        if not self.enabled:
            return False

        if self.event_types is not None and event.event_type not in self.event_types:
            return False

        if self.entity_types is not None and event.entity_type not in self.entity_types:
            return False

        if self.workflow_filter is not None and event.workflow_id != self.workflow_filter:
            return False

        return True


@dataclass
class EventBusStats:
    """Statistics for the event bus."""

    events_published: int = 0
    events_delivered: int = 0
    errors: int = 0
    subscriptions_active: int = 0


class EventBus:
    """
    Thread-safe in-process event bus.

    Supports:
    - Sync and async subscription modes
    - Filtering by event type, entity type, and workflow
    - Error isolation (one handler failure doesn't affect others)
    - Statistics tracking
    """

    def __init__(
        self,
        max_workers: int = 4,
        error_handler: Callable[[str, Event, Exception], None] | None = None,
    ) -> None:
        """
        Initialize the event bus.

        Args:
            max_workers: Maximum worker threads for async handlers.
            error_handler: Optional global error handler.
        """
        self._subscriptions: dict[str, Subscription] = {}
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._error_handler = error_handler
        self._stats = EventBusStats()
        self._shutdown = False

    def subscribe(
        self,
        subscription_id: str,
        handler: Callable[[Event], Any],
        event_types: set[EventType] | None = None,
        entity_types: set[EntityType] | None = None,
        workflow_filter: str | None = None,
        mode: SubscriptionMode = SubscriptionMode.SYNC,
    ) -> None:
        """
        Subscribe to events.

        Args:
            subscription_id: Unique identifier for this subscription.
            handler: Callable that receives events.
            event_types: Only receive these event types (None = all).
            entity_types: Only receive events for these entity types (None = all).
            workflow_filter: Only receive events for this workflow ID.
            mode: SYNC (blocking) or ASYNC (fire-and-forget).
        """
        with self._lock:
            self._subscriptions[subscription_id] = Subscription(
                id=subscription_id,
                handler=handler,
                event_types=event_types,
                entity_types=entity_types,
                workflow_filter=workflow_filter,
                mode=mode,
            )
            self._stats.subscriptions_active = len(self._subscriptions)

    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from events.

        Args:
            subscription_id: The subscription to remove.

        Returns:
            True if subscription was found and removed.
        """
        with self._lock:
            if subscription_id in self._subscriptions:
                del self._subscriptions[subscription_id]
                self._stats.subscriptions_active = len(self._subscriptions)
                return True
            return False

    def enable_subscription(self, subscription_id: str) -> None:
        """Enable a subscription."""
        with self._lock:
            if subscription_id in self._subscriptions:
                self._subscriptions[subscription_id].enabled = True

    def disable_subscription(self, subscription_id: str) -> None:
        """Disable a subscription without removing it."""
        with self._lock:
            if subscription_id in self._subscriptions:
                self._subscriptions[subscription_id].enabled = False

    def publish(self, event: Event) -> None:
        """
        Publish an event to all matching subscribers.

        Sync handlers are called in the current thread.
        Async handlers are submitted to the thread pool.

        Args:
            event: The event to publish.
        """
        if self._shutdown:
            logger.warning("Event bus is shutdown, ignoring event: %s", event.event_id)
            return

        with self._lock:
            self._stats.events_published += 1
            subscriptions = list(self._subscriptions.values())

        for subscription in subscriptions:
            if subscription.matches(event):
                if subscription.mode == SubscriptionMode.SYNC:
                    self._deliver_sync(subscription, event)
                else:
                    self._deliver_async(subscription, event)

    def publish_batch(self, events: list[Event]) -> None:
        """Publish multiple events."""
        for event in events:
            self.publish(event)

    def _deliver_sync(self, subscription: Subscription, event: Event) -> None:
        """Deliver event synchronously."""
        try:
            subscription.handler(event)
            with self._lock:
                self._stats.events_delivered += 1
        except Exception as e:
            self._handle_error(subscription.id, event, e)

    def _deliver_async(self, subscription: Subscription, event: Event) -> None:
        """Deliver event asynchronously via thread pool."""

        def _deliver() -> None:
            try:
                subscription.handler(event)
                with self._lock:
                    self._stats.events_delivered += 1
            except Exception as e:
                self._handle_error(subscription.id, event, e)

        self._executor.submit(_deliver)

    def _handle_error(self, subscription_id: str, event: Event, error: Exception) -> None:
        """Handle a handler error."""
        with self._lock:
            self._stats.errors += 1

        logger.exception(
            "Error in event handler %s for event %s: %s",
            subscription_id,
            event.event_id,
            error,
        )

        if self._error_handler:
            try:
                self._error_handler(subscription_id, event, error)
            except Exception as e:
                logger.exception("Error in error handler: %s", e)

    def on_error(self, handler: Callable[[str, Event, Exception], None]) -> Callable[[str, Event, Exception], None]:
        """
        Set or replace the global error handler.

        Args:
            handler: Callable(subscription_id, event, error)

        Returns:
            The handler (for use as decorator).
        """
        self._error_handler = handler
        return handler

    @property
    def stats(self) -> EventBusStats:
        """Get current statistics."""
        with self._lock:
            return EventBusStats(
                events_published=self._stats.events_published,
                events_delivered=self._stats.events_delivered,
                errors=self._stats.errors,
                subscriptions_active=self._stats.subscriptions_active,
            )

    def get_subscriptions(self) -> list[str]:
        """Get list of subscription IDs."""
        with self._lock:
            return list(self._subscriptions.keys())

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the event bus.

        Args:
            wait: If True, wait for pending async handlers to complete.
        """
        self._shutdown = True
        self._executor.shutdown(wait=wait)

    def reset(self) -> None:
        """Reset statistics and clear subscriptions (for testing)."""
        with self._lock:
            self._subscriptions.clear()
            self._stats = EventBusStats()
            self._shutdown = False


# Global event bus instance
_event_bus: EventBus | None = None
_bus_lock = threading.Lock()


def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    global _event_bus
    if _event_bus is None:
        with _bus_lock:
            if _event_bus is None:
                _event_bus = EventBus()
    return _event_bus


def reset_event_bus() -> None:
    """Reset the global event bus (for testing)."""
    global _event_bus
    with _bus_lock:
        if _event_bus is not None:
            _event_bus.shutdown(wait=False)
        _event_bus = None


def configure_event_bus(
    max_workers: int = 4,
    error_handler: Callable[[str, Event, Exception], None] | None = None,
) -> EventBus:
    """
    Configure the global event bus.

    Must be called before first use of get_event_bus().

    Args:
        max_workers: Maximum worker threads for async handlers.
        error_handler: Optional global error handler.

    Returns:
        The configured event bus.
    """
    global _event_bus
    with _bus_lock:
        if _event_bus is not None:
            _event_bus.shutdown(wait=False)
        _event_bus = EventBus(max_workers=max_workers, error_handler=error_handler)
    return _event_bus
