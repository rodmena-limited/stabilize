"""Tests for event store implementations."""

from datetime import UTC, datetime, timedelta

import pytest

from stabilize.events.base import (
    EntityType,
    Event,
    EventMetadata,
    EventType,
)
from stabilize.events.store import (
    EventQuery,
    InMemoryEventStore,
    SqliteEventStore,
)


class TestInMemoryEventStore:
    """Tests for InMemoryEventStore."""

    @pytest.fixture
    def store(self) -> InMemoryEventStore:
        """Create a fresh in-memory store."""
        return InMemoryEventStore()

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

    def test_append_assigns_sequence(self, store: InMemoryEventStore, sample_event: Event) -> None:
        """Test that append assigns a sequence number."""
        result = store.append(sample_event)
        assert result.sequence == 1
        assert result.event_id == sample_event.event_id

    def test_append_increments_sequence(self, store: InMemoryEventStore) -> None:
        """Test that sequences increment."""
        event1 = Event(event_type=EventType.WORKFLOW_CREATED)
        event2 = Event(event_type=EventType.WORKFLOW_STARTED)

        result1 = store.append(event1)
        result2 = store.append(event2)

        assert result1.sequence == 1
        assert result2.sequence == 2

    def test_append_batch(self, store: InMemoryEventStore) -> None:
        """Test appending multiple events."""
        events = [
            Event(event_type=EventType.WORKFLOW_CREATED, entity_id="w1", workflow_id="w1"),
            Event(event_type=EventType.STAGE_STARTED, entity_id="s1", workflow_id="w1"),
            Event(event_type=EventType.TASK_STARTED, entity_id="t1", workflow_id="w1"),
        ]

        results = store.append_batch(events)

        assert len(results) == 3
        assert results[0].sequence == 1
        assert results[1].sequence == 2
        assert results[2].sequence == 3

    def test_get_events_for_workflow(self, store: InMemoryEventStore) -> None:
        """Test getting events for a specific workflow."""
        # Add events for two workflows
        events = [
            Event(event_type=EventType.WORKFLOW_CREATED, entity_id="w1", workflow_id="w1"),
            Event(event_type=EventType.WORKFLOW_CREATED, entity_id="w2", workflow_id="w2"),
            Event(event_type=EventType.STAGE_STARTED, entity_id="s1", workflow_id="w1"),
        ]
        store.append_batch(events)

        # Query w1
        results = store.get_events_for_workflow("w1")

        assert len(results) == 2
        assert all(e.workflow_id == "w1" for e in results)

    def test_get_events_for_entity(self, store: InMemoryEventStore) -> None:
        """Test getting events for a specific entity."""
        events = [
            Event(
                event_type=EventType.STAGE_STARTED,
                entity_type=EntityType.STAGE,
                entity_id="stage-1",
                workflow_id="w1",
                version=1,
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                entity_id="stage-1",
                workflow_id="w1",
                version=2,
            ),
            Event(
                event_type=EventType.STAGE_STARTED,
                entity_type=EntityType.STAGE,
                entity_id="stage-2",
                workflow_id="w1",
                version=1,
            ),
        ]
        store.append_batch(events)

        results = store.get_events_for_entity(EntityType.STAGE, "stage-1")

        assert len(results) == 2
        assert all(e.entity_id == "stage-1" for e in results)

    def test_get_events_since(self, store: InMemoryEventStore) -> None:
        """Test getting events since a sequence."""
        events = [Event(event_type=EventType.WORKFLOW_CREATED) for _ in range(5)]
        store.append_batch(events)

        results = store.get_events_since(2)

        assert len(results) == 3
        assert results[0].sequence == 3
        assert results[-1].sequence == 5

    def test_get_current_sequence(self, store: InMemoryEventStore) -> None:
        """Test getting current sequence."""
        assert store.get_current_sequence() == 0

        store.append(Event(event_type=EventType.WORKFLOW_CREATED))
        assert store.get_current_sequence() == 1

        store.append(Event(event_type=EventType.WORKFLOW_STARTED))
        assert store.get_current_sequence() == 2

    def test_count_events(self, store: InMemoryEventStore) -> None:
        """Test counting events."""
        assert store.count_events() == 0

        events = [Event(event_type=EventType.WORKFLOW_CREATED) for _ in range(10)]
        store.append_batch(events)

        assert store.count_events() == 10

    def test_get_event_by_id(self, store: InMemoryEventStore, sample_event: Event) -> None:
        """Test getting event by ID."""
        store.append(sample_event)

        result = store.get_event_by_id(sample_event.event_id)
        assert result is not None
        assert result.event_id == sample_event.event_id

        # Non-existent
        result = store.get_event_by_id("non-existent")
        assert result is None

    def test_clear(self, store: InMemoryEventStore) -> None:
        """Test clearing the store."""
        events = [Event(event_type=EventType.WORKFLOW_CREATED) for _ in range(5)]
        store.append_batch(events)

        store.clear()

        assert store.count_events() == 0
        assert store.get_current_sequence() == 0

    def test_query_with_event_types(self, store: InMemoryEventStore) -> None:
        """Test querying by event types."""
        events = [
            Event(event_type=EventType.WORKFLOW_CREATED),
            Event(event_type=EventType.WORKFLOW_STARTED),
            Event(event_type=EventType.STAGE_STARTED),
            Event(event_type=EventType.WORKFLOW_COMPLETED),
        ]
        store.append_batch(events)

        query = EventQuery(event_types=[EventType.WORKFLOW_CREATED, EventType.WORKFLOW_COMPLETED])
        results = list(store.get_events(query))

        assert len(results) == 2
        assert results[0].event_type == EventType.WORKFLOW_CREATED
        assert results[1].event_type == EventType.WORKFLOW_COMPLETED


class TestSqliteEventStore:
    """Tests for SqliteEventStore."""

    @pytest.fixture
    def store(self) -> SqliteEventStore:
        """Create a fresh SQLite store in memory."""
        return SqliteEventStore("sqlite:///:memory:", create_tables=True)

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

    def test_append_assigns_sequence(self, store: SqliteEventStore, sample_event: Event) -> None:
        """Test that append assigns a sequence number."""
        result = store.append(sample_event)
        assert result.sequence > 0
        assert result.event_id == sample_event.event_id

    def test_append_increments_sequence(self, store: SqliteEventStore) -> None:
        """Test that sequences increment."""
        event1 = Event(event_type=EventType.WORKFLOW_CREATED)
        event2 = Event(event_type=EventType.WORKFLOW_STARTED)

        result1 = store.append(event1)
        result2 = store.append(event2)

        assert result2.sequence > result1.sequence

    def test_append_batch_atomic(self, store: SqliteEventStore) -> None:
        """Test that batch append is atomic."""
        events = [
            Event(event_type=EventType.WORKFLOW_CREATED, entity_id="w1", workflow_id="w1"),
            Event(event_type=EventType.STAGE_STARTED, entity_id="s1", workflow_id="w1"),
        ]

        results = store.append_batch(events)

        assert len(results) == 2
        # Sequences should be consecutive
        assert results[1].sequence == results[0].sequence + 1

    def test_get_events_for_workflow(self, store: SqliteEventStore) -> None:
        """Test getting events for a workflow."""
        events = [
            Event(event_type=EventType.WORKFLOW_CREATED, entity_id="w1", workflow_id="w1"),
            Event(event_type=EventType.WORKFLOW_CREATED, entity_id="w2", workflow_id="w2"),
            Event(event_type=EventType.STAGE_STARTED, entity_id="s1", workflow_id="w1"),
        ]
        store.append_batch(events)

        results = store.get_events_for_workflow("w1")

        assert len(results) == 2

    def test_get_events_since(self, store: SqliteEventStore) -> None:
        """Test getting events since sequence."""
        events = [Event(event_type=EventType.WORKFLOW_CREATED) for _ in range(5)]
        results = store.append_batch(events)

        # Get events after sequence 2
        after_seq = results[1].sequence
        fetched = store.get_events_since(after_seq)

        assert len(fetched) == 3

    def test_data_serialization(self, store: SqliteEventStore) -> None:
        """Test that data is properly serialized/deserialized."""
        event = Event(
            event_type=EventType.WORKFLOW_CREATED,
            workflow_id="w1",
            entity_id="w1",
            data={
                "application": "test",
                "stages": ["stage1", "stage2"],
                "config": {"key": "value"},
            },
        )

        store.append(event)
        retrieved = store.get_events_for_workflow("w1")[0]

        assert retrieved.data["application"] == "test"
        assert retrieved.data["stages"] == ["stage1", "stage2"]
        assert retrieved.data["config"] == {"key": "value"}

    def test_snapshot_operations(self, store: SqliteEventStore) -> None:
        """Test snapshot save and retrieve."""
        store.save_snapshot(
            entity_type=EntityType.WORKFLOW,
            entity_id="workflow-123",
            workflow_id="workflow-123",
            version=5,
            sequence=100,
            state={"status": "RUNNING", "stages": ["s1", "s2"]},
        )

        snapshot = store.get_latest_snapshot(EntityType.WORKFLOW, "workflow-123")

        assert snapshot is not None
        assert snapshot["version"] == 5
        assert snapshot["sequence"] == 100
        assert snapshot["state"]["status"] == "RUNNING"

    def test_subscription_operations(self, store: SqliteEventStore) -> None:
        """Test subscription save and retrieve."""
        store.save_subscription(
            subscription_id="sub-1",
            event_types=[EventType.WORKFLOW_COMPLETED],
            entity_filter={"workflow_id": "w1"},
            last_sequence=50,
            webhook_url="https://example.com/hook",
        )

        sub = store.get_subscription("sub-1")

        assert sub is not None
        assert sub["last_sequence"] == 50
        assert sub["webhook_url"] == "https://example.com/hook"

        # Update sequence
        store.update_subscription_sequence("sub-1", 75)
        sub = store.get_subscription("sub-1")
        assert sub["last_sequence"] == 75

        # Delete
        store.delete_subscription("sub-1")
        sub = store.get_subscription("sub-1")
        assert sub is None


class TestEventQuery:
    """Tests for EventQuery."""

    def test_default_values(self) -> None:
        """Test default query values."""
        query = EventQuery()

        assert query.entity_type is None
        assert query.entity_id is None
        assert query.workflow_id is None
        assert query.event_types is None
        assert query.from_sequence is None
        assert query.to_sequence is None
        assert query.limit == 1000
        assert query.offset == 0
        assert query.ascending is True

    def test_filter_by_entity(self) -> None:
        """Test entity filtering."""
        query = EventQuery(
            entity_type=EntityType.STAGE,
            entity_id="stage-123",
        )

        assert query.entity_type == EntityType.STAGE
        assert query.entity_id == "stage-123"

    def test_sequence_range(self) -> None:
        """Test sequence range filtering."""
        query = EventQuery(
            from_sequence=100,
            to_sequence=200,
        )

        assert query.from_sequence == 100
        assert query.to_sequence == 200

    def test_time_range(self) -> None:
        """Test time range filtering."""
        start = datetime.now(UTC) - timedelta(hours=1)
        end = datetime.now(UTC)

        query = EventQuery(
            from_timestamp=start,
            to_timestamp=end,
        )

        assert query.from_timestamp == start
        assert query.to_timestamp == end
