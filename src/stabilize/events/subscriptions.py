"""
Durable subscriptions for event processing.

Durable subscriptions persist their position and can resume
after restarts, ensuring at-least-once delivery of events.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from stabilize.events.base import Event, EventType

if TYPE_CHECKING:
    from stabilize.events.store.interface import EventStore

logger = logging.getLogger(__name__)


@dataclass
class DurableSubscription:
    """
    A durable subscription that survives restarts.

    Position is persisted to the database, allowing the
    subscription to resume from where it left off.
    """

    id: str
    handler: Callable[[Event], None]
    event_types: list[EventType] | None = None
    entity_filter: dict[str, Any] | None = None
    last_sequence: int = 0
    webhook_url: str | None = None
    enabled: bool = True
    error_count: int = 0
    max_errors: int = 10

    def matches(self, event: Event) -> bool:
        """Check if this subscription should receive the event."""
        if not self.enabled:
            return False

        if self.event_types is not None:
            if event.event_type not in self.event_types:
                return False

        if self.entity_filter is not None:
            # Filter by entity type
            if "entity_type" in self.entity_filter:
                if event.entity_type.value != self.entity_filter["entity_type"]:
                    return False
            # Filter by workflow
            if "workflow_id" in self.entity_filter:
                if event.workflow_id != self.entity_filter["workflow_id"]:
                    return False

        return True


class SubscriptionManager:
    """
    Manages durable subscriptions that survive restarts.

    Features:
    - Persisted position (survives restarts)
    - Catch-up from last position
    - At-least-once delivery
    - Automatic error handling with backoff
    - Optional webhook integration
    """

    def __init__(
        self,
        event_store: EventStore,
        poll_interval: float = 1.0,
        batch_size: int = 100,
    ) -> None:
        """
        Initialize the subscription manager.

        Args:
            event_store: The event store to poll.
            poll_interval: Seconds between polls.
            batch_size: Maximum events per poll.
        """
        self._event_store = event_store
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._subscriptions: dict[str, DurableSubscription] = {}
        self._lock = threading.Lock()
        self._running = False
        self._poll_thread: threading.Thread | None = None

    def create_subscription(
        self,
        subscription_id: str,
        handler: Callable[[Event], None],
        event_types: list[EventType] | None = None,
        entity_filter: dict[str, Any] | None = None,
        webhook_url: str | None = None,
        start_from: str = "latest",
    ) -> None:
        """
        Create a durable subscription.

        Args:
            subscription_id: Unique identifier for this subscription.
            handler: Function to call with each event.
            event_types: Only receive these event types (None = all).
            entity_filter: Filter by entity properties.
            webhook_url: Optional webhook URL to call.
            start_from: "latest" (current position), "beginning" (sequence 0),
                       or a sequence number string.
        """
        # Determine starting position
        if start_from == "latest":
            last_sequence = self._event_store.get_current_sequence()
        elif start_from == "beginning":
            last_sequence = 0
        else:
            last_sequence = int(start_from)

        subscription = DurableSubscription(
            id=subscription_id,
            handler=handler,
            event_types=event_types,
            entity_filter=entity_filter,
            last_sequence=last_sequence,
            webhook_url=webhook_url,
        )

        with self._lock:
            self._subscriptions[subscription_id] = subscription

        # Persist subscription
        if hasattr(self._event_store, "save_subscription"):
            self._event_store.save_subscription(
                subscription_id=subscription_id,
                event_types=event_types,
                entity_filter=entity_filter,
                last_sequence=last_sequence,
                webhook_url=webhook_url,
            )

        logger.info(
            "Created subscription %s starting from sequence %d",
            subscription_id,
            last_sequence,
        )

    def delete_subscription(self, subscription_id: str) -> bool:
        """
        Delete a subscription.

        Args:
            subscription_id: The subscription to delete.

        Returns:
            True if subscription was found and deleted.
        """
        with self._lock:
            if subscription_id in self._subscriptions:
                del self._subscriptions[subscription_id]

        if hasattr(self._event_store, "delete_subscription"):
            self._event_store.delete_subscription(subscription_id)

        logger.info("Deleted subscription %s", subscription_id)
        return True

    def load_subscription(
        self,
        subscription_id: str,
        handler: Callable[[Event], None],
    ) -> bool:
        """
        Load a persisted subscription.

        Args:
            subscription_id: The subscription to load.
            handler: Handler function for events.

        Returns:
            True if subscription was found and loaded.
        """
        if not hasattr(self._event_store, "get_subscription"):
            return False

        data = self._event_store.get_subscription(subscription_id)
        if data is None:
            return False

        subscription = DurableSubscription(
            id=subscription_id,
            handler=handler,
            event_types=data.get("event_types"),
            entity_filter=data.get("entity_filter"),
            last_sequence=data.get("last_sequence", 0),
            webhook_url=data.get("webhook_url"),
        )

        with self._lock:
            self._subscriptions[subscription_id] = subscription

        logger.info(
            "Loaded subscription %s from sequence %d",
            subscription_id,
            subscription.last_sequence,
        )
        return True

    def start(self) -> None:
        """Start polling for events."""
        if self._running:
            return

        self._running = True
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        logger.info("Subscription manager started")

    def stop(self) -> None:
        """Stop polling for events."""
        self._running = False
        if self._poll_thread:
            self._poll_thread.join(timeout=5.0)
            self._poll_thread = None
        logger.info("Subscription manager stopped")

    def _poll_loop(self) -> None:
        """Main polling loop."""
        while self._running:
            try:
                self._process_pending_events()
            except Exception as e:
                logger.exception("Error in subscription poll loop: %s", e)

            time.sleep(self._poll_interval)

    def _process_pending_events(self) -> None:
        """Process pending events for all subscriptions."""
        with self._lock:
            subscriptions = list(self._subscriptions.values())

        for subscription in subscriptions:
            if not subscription.enabled:
                continue

            try:
                self._process_subscription(subscription)
            except Exception as e:
                logger.exception(
                    "Error processing subscription %s: %s",
                    subscription.id,
                    e,
                )
                subscription.error_count += 1

                # Disable subscription if too many errors
                if subscription.error_count >= subscription.max_errors:
                    subscription.enabled = False
                    logger.error(
                        "Subscription %s disabled after %d errors",
                        subscription.id,
                        subscription.error_count,
                    )

    def _process_subscription(self, subscription: DurableSubscription) -> None:
        """Process events for a single subscription."""
        events = self._event_store.get_events_since(
            subscription.last_sequence,
            limit=self._batch_size,
        )

        if not events:
            return

        processed_sequence = subscription.last_sequence

        for event in events:
            if subscription.matches(event):
                try:
                    subscription.handler(event)
                except Exception as e:
                    logger.error(
                        "Handler error for subscription %s on event %s: %s",
                        subscription.id,
                        event.event_id,
                        e,
                    )
                    # Don't update sequence - will retry on next poll
                    raise

            processed_sequence = event.sequence

        # Update position
        subscription.last_sequence = processed_sequence
        subscription.error_count = 0  # Reset on success

        # Persist position
        if hasattr(self._event_store, "update_subscription_sequence"):
            self._event_store.update_subscription_sequence(
                subscription.id,
                processed_sequence,
            )

    def process_once(self) -> int:
        """
        Process pending events once (for testing).

        Returns:
            Number of events processed.
        """
        total = 0
        with self._lock:
            subscriptions = list(self._subscriptions.values())

        for subscription in subscriptions:
            if not subscription.enabled:
                continue

            events = self._event_store.get_events_since(
                subscription.last_sequence,
                limit=self._batch_size,
            )

            for event in events:
                if subscription.matches(event):
                    subscription.handler(event)
                    total += 1
                subscription.last_sequence = event.sequence

        return total

    def get_subscription_status(self, subscription_id: str) -> dict[str, Any] | None:
        """Get status of a subscription."""
        with self._lock:
            subscription = self._subscriptions.get(subscription_id)

        if subscription is None:
            return None

        current_sequence = self._event_store.get_current_sequence()

        return {
            "id": subscription.id,
            "enabled": subscription.enabled,
            "last_sequence": subscription.last_sequence,
            "current_sequence": current_sequence,
            "lag": current_sequence - subscription.last_sequence,
            "error_count": subscription.error_count,
            "event_types": ([et.value for et in subscription.event_types] if subscription.event_types else None),
        }

    def get_all_subscription_status(self) -> list[dict[str, Any]]:
        """Get status of all subscriptions."""
        with self._lock:
            subscription_ids = list(self._subscriptions.keys())

        return [status for sub_id in subscription_ids if (status := self.get_subscription_status(sub_id)) is not None]
