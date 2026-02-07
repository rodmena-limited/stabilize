"""Tests for event base types."""

from datetime import datetime

import pytest

from stabilize.events.base import (
    EntityType,
    Event,
    EventMetadata,
    EventType,
    create_stage_event,
    create_task_event,
    create_workflow_event,
)


class TestEventMetadata:
    """Tests for EventMetadata."""

    def test_create_metadata(self) -> None:
        """Test creating event metadata."""
        metadata = EventMetadata(
            correlation_id="workflow-123",
            causation_id="event-456",
            actor="user@example.com",
            source_handler="StartWorkflowHandler",
        )

        assert metadata.correlation_id == "workflow-123"
        assert metadata.causation_id == "event-456"
        assert metadata.actor == "user@example.com"
        assert metadata.source_handler == "StartWorkflowHandler"

    def test_metadata_defaults(self) -> None:
        """Test default values."""
        metadata = EventMetadata(correlation_id="test")

        assert metadata.causation_id is None
        assert metadata.actor == "system"
        assert metadata.source_handler is None

    def test_metadata_to_dict(self) -> None:
        """Test serialization."""
        metadata = EventMetadata(
            correlation_id="workflow-123",
            actor="admin",
        )

        data = metadata.to_dict()
        assert data["correlation_id"] == "workflow-123"
        assert data["actor"] == "admin"

    def test_metadata_from_dict(self) -> None:
        """Test deserialization."""
        data = {
            "correlation_id": "workflow-123",
            "causation_id": "event-456",
            "actor": "user",
            "source_handler": "TestHandler",
        }

        metadata = EventMetadata.from_dict(data)
        assert metadata.correlation_id == "workflow-123"
        assert metadata.causation_id == "event-456"


class TestEvent:
    """Tests for Event model."""

    def test_create_event(self) -> None:
        """Test creating an event."""
        metadata = EventMetadata(correlation_id="workflow-123")
        event = Event(
            event_type=EventType.WORKFLOW_CREATED,
            entity_type=EntityType.WORKFLOW,
            entity_id="workflow-123",
            workflow_id="workflow-123",
            version=1,
            data={"application": "test-app"},
            metadata=metadata,
        )

        assert event.event_type == EventType.WORKFLOW_CREATED
        assert event.entity_type == EntityType.WORKFLOW
        assert event.entity_id == "workflow-123"
        assert event.data["application"] == "test-app"

    def test_event_has_id(self) -> None:
        """Test that events get a unique ID."""
        event = Event()
        assert event.event_id is not None
        assert len(event.event_id) > 0

    def test_event_has_timestamp(self) -> None:
        """Test that events get a timestamp."""
        event = Event()
        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)

    def test_event_immutable(self) -> None:
        """Test that events are immutable."""
        event = Event()
        with pytest.raises(Exception):  # FrozenInstanceError
            event.event_id = "new-id"  # type: ignore

    def test_event_with_sequence(self) -> None:
        """Test creating event with sequence."""
        event = Event(event_type=EventType.WORKFLOW_CREATED)
        assert event.sequence == 0

        new_event = event.with_sequence(42)
        assert new_event.sequence == 42
        assert event.sequence == 0  # Original unchanged
        assert new_event.event_id == event.event_id  # Same ID

    def test_event_to_dict(self) -> None:
        """Test event serialization."""
        metadata = EventMetadata(correlation_id="workflow-123")
        event = Event(
            event_type=EventType.STAGE_STARTED,
            entity_type=EntityType.STAGE,
            entity_id="stage-456",
            workflow_id="workflow-123",
            version=2,
            data={"name": "test-stage"},
            metadata=metadata,
        )

        data = event.to_dict()
        assert data["event_type"] == "stage.started"
        assert data["entity_type"] == "stage"
        assert data["entity_id"] == "stage-456"
        assert data["version"] == 2

    def test_event_from_dict(self) -> None:
        """Test event deserialization."""
        data = {
            "event_id": "test-123",
            "event_type": "workflow.completed",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "sequence": 42,
            "entity_type": "workflow",
            "entity_id": "workflow-123",
            "workflow_id": "workflow-123",
            "version": 3,
            "data": {"status": "SUCCEEDED"},
            "metadata": {"correlation_id": "workflow-123"},
        }

        event = Event.from_dict(data)
        assert event.event_id == "test-123"
        assert event.event_type == EventType.WORKFLOW_COMPLETED
        assert event.sequence == 42
        assert event.data["status"] == "SUCCEEDED"


class TestEventFactories:
    """Tests for event factory functions."""

    def test_create_workflow_event(self) -> None:
        """Test workflow event factory."""
        metadata = EventMetadata(correlation_id="workflow-123")
        event = create_workflow_event(
            event_type=EventType.WORKFLOW_STARTED,
            workflow_id="workflow-123",
            version=1,
            data={"start_time": 1234567890},
            metadata=metadata,
        )

        assert event.event_type == EventType.WORKFLOW_STARTED
        assert event.entity_type == EntityType.WORKFLOW
        assert event.entity_id == "workflow-123"
        assert event.workflow_id == "workflow-123"

    def test_create_stage_event(self) -> None:
        """Test stage event factory."""
        metadata = EventMetadata(correlation_id="workflow-123")
        event = create_stage_event(
            event_type=EventType.STAGE_COMPLETED,
            stage_id="stage-456",
            workflow_id="workflow-123",
            version=2,
            data={"status": "SUCCEEDED"},
            metadata=metadata,
        )

        assert event.event_type == EventType.STAGE_COMPLETED
        assert event.entity_type == EntityType.STAGE
        assert event.entity_id == "stage-456"
        assert event.workflow_id == "workflow-123"

    def test_create_task_event(self) -> None:
        """Test task event factory."""
        metadata = EventMetadata(correlation_id="workflow-123")
        event = create_task_event(
            event_type=EventType.TASK_FAILED,
            task_id="task-789",
            workflow_id="workflow-123",
            version=1,
            data={"error": "Something went wrong"},
            metadata=metadata,
        )

        assert event.event_type == EventType.TASK_FAILED
        assert event.entity_type == EntityType.TASK
        assert event.entity_id == "task-789"


class TestEventTypes:
    """Tests for EventType enum."""

    def test_all_event_types_have_values(self) -> None:
        """Test that all event types have string values."""
        for event_type in EventType:
            assert isinstance(event_type.value, str)
            assert "." in event_type.value  # Format: category.action

    def test_workflow_events(self) -> None:
        """Test workflow lifecycle events."""
        assert EventType.WORKFLOW_CREATED.value == "workflow.created"
        assert EventType.WORKFLOW_STARTED.value == "workflow.started"
        assert EventType.WORKFLOW_COMPLETED.value == "workflow.completed"
        assert EventType.WORKFLOW_FAILED.value == "workflow.failed"
        assert EventType.WORKFLOW_CANCELED.value == "workflow.canceled"

    def test_stage_events(self) -> None:
        """Test stage lifecycle events."""
        assert EventType.STAGE_STARTED.value == "stage.started"
        assert EventType.STAGE_COMPLETED.value == "stage.completed"
        assert EventType.STAGE_FAILED.value == "stage.failed"
        assert EventType.STAGE_SKIPPED.value == "stage.skipped"

    def test_task_events(self) -> None:
        """Test task lifecycle events."""
        assert EventType.TASK_STARTED.value == "task.started"
        assert EventType.TASK_COMPLETED.value == "task.completed"
        assert EventType.TASK_FAILED.value == "task.failed"
        assert EventType.TASK_RETRIED.value == "task.retried"
