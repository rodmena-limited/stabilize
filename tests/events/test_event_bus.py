"""Tests for event bus."""

from threading import Event as ThreadEvent

import pytest

from stabilize.events.base import (
    EntityType,
    Event,
    EventMetadata,
    EventType,
)
from stabilize.events.bus import (
    EventBus,
    SubscriptionMode,
    get_event_bus,
    reset_event_bus,
)


@pytest.fixture(autouse=True)
def reset_bus() -> None:
    """Reset global event bus before each test."""
    reset_event_bus()


class TestEventBus:
    """Tests for EventBus."""

    @pytest.fixture
    def bus(self) -> EventBus:
        """Create a fresh event bus."""
        return EventBus()

    @pytest.fixture
    def sample_event(self) -> Event:
        """Create a sample event."""
        return Event(
            event_type=EventType.WORKFLOW_CREATED,
            entity_type=EntityType.WORKFLOW,
            entity_id="workflow-123",
            workflow_id="workflow-123",
            version=1,
            data={"application": "test-app"},
            metadata=EventMetadata(correlation_id="workflow-123"),
        )

    def test_subscribe_and_publish(self, bus: EventBus, sample_event: Event) -> None:
        """Test basic subscribe and publish."""
        received: list[Event] = []

        bus.subscribe("test", lambda e: received.append(e))
        bus.publish(sample_event)

        assert len(received) == 1
        assert received[0].event_id == sample_event.event_id

    def test_multiple_subscribers(self, bus: EventBus, sample_event: Event) -> None:
        """Test multiple subscribers receive same event."""
        received1: list[Event] = []
        received2: list[Event] = []

        bus.subscribe("sub1", lambda e: received1.append(e))
        bus.subscribe("sub2", lambda e: received2.append(e))
        bus.publish(sample_event)

        assert len(received1) == 1
        assert len(received2) == 1

    def test_filter_by_event_type(self, bus: EventBus) -> None:
        """Test filtering by event type."""
        received: list[Event] = []

        bus.subscribe(
            "test",
            lambda e: received.append(e),
            event_types={EventType.WORKFLOW_COMPLETED},
        )

        # Publish different events
        bus.publish(Event(event_type=EventType.WORKFLOW_CREATED))
        bus.publish(Event(event_type=EventType.WORKFLOW_COMPLETED))
        bus.publish(Event(event_type=EventType.STAGE_STARTED))

        assert len(received) == 1
        assert received[0].event_type == EventType.WORKFLOW_COMPLETED

    def test_filter_by_entity_type(self, bus: EventBus) -> None:
        """Test filtering by entity type."""
        received: list[Event] = []

        bus.subscribe(
            "test",
            lambda e: received.append(e),
            entity_types={EntityType.STAGE},
        )

        bus.publish(Event(event_type=EventType.WORKFLOW_CREATED, entity_type=EntityType.WORKFLOW))
        bus.publish(Event(event_type=EventType.STAGE_STARTED, entity_type=EntityType.STAGE))
        bus.publish(Event(event_type=EventType.TASK_STARTED, entity_type=EntityType.TASK))

        assert len(received) == 1
        assert received[0].entity_type == EntityType.STAGE

    def test_filter_by_workflow(self, bus: EventBus) -> None:
        """Test filtering by workflow ID."""
        received: list[Event] = []

        bus.subscribe(
            "test",
            lambda e: received.append(e),
            workflow_filter="workflow-123",
        )

        bus.publish(Event(workflow_id="workflow-123"))
        bus.publish(Event(workflow_id="workflow-456"))
        bus.publish(Event(workflow_id="workflow-123"))

        assert len(received) == 2
        assert all(e.workflow_id == "workflow-123" for e in received)

    def test_unsubscribe(self, bus: EventBus, sample_event: Event) -> None:
        """Test unsubscribing."""
        received: list[Event] = []

        bus.subscribe("test", lambda e: received.append(e))
        bus.publish(sample_event)
        assert len(received) == 1

        bus.unsubscribe("test")
        bus.publish(sample_event)
        assert len(received) == 1  # No new events

    def test_disable_subscription(self, bus: EventBus, sample_event: Event) -> None:
        """Test disabling a subscription."""
        received: list[Event] = []

        bus.subscribe("test", lambda e: received.append(e))
        bus.publish(sample_event)
        assert len(received) == 1

        bus.disable_subscription("test")
        bus.publish(sample_event)
        assert len(received) == 1  # No new events

        bus.enable_subscription("test")
        bus.publish(sample_event)
        assert len(received) == 2  # Enabled again

    def test_async_subscription(self, bus: EventBus, sample_event: Event) -> None:
        """Test async subscription mode."""
        received: list[Event] = []
        done = ThreadEvent()

        def handler(e: Event) -> None:
            received.append(e)
            done.set()

        bus.subscribe("test", handler, mode=SubscriptionMode.ASYNC)
        bus.publish(sample_event)

        # Wait for async delivery
        done.wait(timeout=2.0)

        assert len(received) == 1

    def test_error_isolation(self, bus: EventBus, sample_event: Event) -> None:
        """Test that handler errors don't affect other handlers."""
        received: list[Event] = []

        def failing_handler(e: Event) -> None:
            raise RuntimeError("Handler error")

        bus.subscribe("failing", failing_handler)
        bus.subscribe("working", lambda e: received.append(e))

        bus.publish(sample_event)

        # Working handler should still receive event
        assert len(received) == 1

    def test_error_handler(self, bus: EventBus, sample_event: Event) -> None:
        """Test custom error handler."""
        errors: list[tuple[str, Event, Exception]] = []

        bus.on_error(lambda sub_id, event, error: errors.append((sub_id, event, error)))

        def failing_handler(e: Event) -> None:
            raise RuntimeError("Handler error")

        bus.subscribe("failing", failing_handler)
        bus.publish(sample_event)

        assert len(errors) == 1
        assert errors[0][0] == "failing"
        assert isinstance(errors[0][2], RuntimeError)

    def test_publish_batch(self, bus: EventBus) -> None:
        """Test publishing multiple events."""
        received: list[Event] = []

        bus.subscribe("test", lambda e: received.append(e))

        events = [
            Event(event_type=EventType.WORKFLOW_CREATED),
            Event(event_type=EventType.WORKFLOW_STARTED),
            Event(event_type=EventType.WORKFLOW_COMPLETED),
        ]
        bus.publish_batch(events)

        assert len(received) == 3

    def test_stats(self, bus: EventBus, sample_event: Event) -> None:
        """Test event bus statistics."""
        bus.subscribe("test", lambda e: None)
        bus.publish(sample_event)
        bus.publish(sample_event)

        stats = bus.stats
        assert stats.events_published == 2
        assert stats.events_delivered == 2
        assert stats.subscriptions_active == 1
        assert stats.errors == 0

    def test_get_subscriptions(self, bus: EventBus) -> None:
        """Test getting subscription list."""
        bus.subscribe("sub1", lambda e: None)
        bus.subscribe("sub2", lambda e: None)
        bus.subscribe("sub3", lambda e: None)

        subs = bus.get_subscriptions()

        assert "sub1" in subs
        assert "sub2" in subs
        assert "sub3" in subs

    def test_shutdown(self, bus: EventBus, sample_event: Event) -> None:
        """Test shutdown."""
        received: list[Event] = []

        bus.subscribe("test", lambda e: received.append(e))
        bus.shutdown()

        # Events should be ignored after shutdown
        bus.publish(sample_event)
        assert len(received) == 0

    def test_reset(self, bus: EventBus, sample_event: Event) -> None:
        """Test reset."""
        bus.subscribe("test", lambda e: None)
        bus.publish(sample_event)

        bus.reset()

        assert bus.stats.events_published == 0
        assert bus.stats.subscriptions_active == 0


class TestGlobalEventBus:
    """Tests for global event bus."""

    def test_get_event_bus_singleton(self) -> None:
        """Test that get_event_bus returns singleton."""
        bus1 = get_event_bus()
        bus2 = get_event_bus()

        assert bus1 is bus2

    def test_reset_event_bus(self) -> None:
        """Test resetting global event bus."""
        bus1 = get_event_bus()
        reset_event_bus()
        bus2 = get_event_bus()

        assert bus1 is not bus2
