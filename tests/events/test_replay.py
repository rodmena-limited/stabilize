"""Tests for event replay."""

import pytest

from stabilize.events.base import (
    EntityType,
    Event,
    EventMetadata,
    EventType,
)
from stabilize.events.replay import EventReplayer, WorkflowState
from stabilize.events.store import InMemoryEventStore


class TestEventReplayer:
    """Tests for EventReplayer."""

    @pytest.fixture
    def store(self) -> InMemoryEventStore:
        """Create a fresh in-memory store."""
        return InMemoryEventStore()

    @pytest.fixture
    def replayer(self, store: InMemoryEventStore) -> EventReplayer:
        """Create an event replayer."""
        return EventReplayer(store)

    @pytest.fixture
    def workflow_events(self, store: InMemoryEventStore) -> list[Event]:
        """Create and store a sequence of workflow events."""
        metadata = EventMetadata(correlation_id="workflow-123")
        events = [
            Event(
                event_type=EventType.WORKFLOW_CREATED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-123",
                workflow_id="workflow-123",
                version=1,
                data={"application": "test-app", "name": "test-workflow"},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.WORKFLOW_STARTED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-123",
                workflow_id="workflow-123",
                version=2,
                data={"start_time": 1000, "context": {"key": "value"}},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.STAGE_STARTED,
                entity_type=EntityType.STAGE,
                entity_id="stage-1",
                workflow_id="workflow-123",
                version=1,
                data={"name": "build", "type": "shell"},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.TASK_STARTED,
                entity_type=EntityType.TASK,
                entity_id="task-1",
                workflow_id="workflow-123",
                version=1,
                data={"name": "compile", "stage_id": "stage-1"},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.TASK_COMPLETED,
                entity_type=EntityType.TASK,
                entity_id="task-1",
                workflow_id="workflow-123",
                version=2,
                data={
                    "name": "compile",
                    "status": "SUCCEEDED",
                    "outputs": {"artifact": "build.jar"},
                },
                metadata=metadata,
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                entity_id="stage-1",
                workflow_id="workflow-123",
                version=2,
                data={
                    "name": "build",
                    "status": "SUCCEEDED",
                    "outputs": {"artifact": "build.jar"},
                },
                metadata=metadata,
            ),
            Event(
                event_type=EventType.WORKFLOW_COMPLETED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-123",
                workflow_id="workflow-123",
                version=3,
                data={"status": "SUCCEEDED"},
                metadata=metadata,
            ),
        ]
        return store.append_batch(events)

    def test_rebuild_workflow_state(
        self,
        replayer: EventReplayer,
        workflow_events: list[Event],
    ) -> None:
        """Test rebuilding complete workflow state."""
        state = replayer.rebuild_workflow_state("workflow-123")

        assert state["workflow_id"] == "workflow-123"
        assert state["status"] == "SUCCEEDED"
        assert state["application"] == "test-app"
        assert state["name"] == "test-workflow"

    def test_rebuild_includes_stages(
        self,
        replayer: EventReplayer,
        workflow_events: list[Event],
    ) -> None:
        """Test that rebuilt state includes stages."""
        state = replayer.rebuild_workflow_state("workflow-123")

        assert "stage-1" in state["stages"]
        stage = state["stages"]["stage-1"]
        assert stage["name"] == "build"
        assert stage["status"] == "SUCCEEDED"

    def test_rebuild_includes_tasks(
        self,
        replayer: EventReplayer,
        workflow_events: list[Event],
    ) -> None:
        """Test that rebuilt state includes tasks."""
        state = replayer.rebuild_workflow_state("workflow-123")

        assert "task-1" in state["tasks"]
        task = state["tasks"]["task-1"]
        assert task["name"] == "compile"
        assert task["status"] == "SUCCEEDED"

    def test_rebuild_includes_context(
        self,
        replayer: EventReplayer,
        workflow_events: list[Event],
    ) -> None:
        """Test that rebuilt state includes context."""
        state = replayer.rebuild_workflow_state("workflow-123")

        assert state["context"]["key"] == "value"

    def test_rebuild_as_of_sequence(
        self,
        replayer: EventReplayer,
        store: InMemoryEventStore,
    ) -> None:
        """Test rebuilding state as of a specific sequence."""
        metadata = EventMetadata(correlation_id="workflow-456")

        events = [
            Event(
                event_type=EventType.WORKFLOW_CREATED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-456",
                workflow_id="workflow-456",
                data={"name": "test"},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.WORKFLOW_STARTED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-456",
                workflow_id="workflow-456",
                metadata=metadata,
            ),
            Event(
                event_type=EventType.WORKFLOW_COMPLETED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-456",
                workflow_id="workflow-456",
                data={"status": "SUCCEEDED"},
                metadata=metadata,
            ),
        ]
        stored = store.append_batch(events)

        # Get state after STARTED but before COMPLETED
        state = replayer.rebuild_workflow_state(
            "workflow-456",
            as_of_sequence=stored[1].sequence,
        )

        assert state["status"] == "RUNNING"

        # Get full state
        state = replayer.rebuild_workflow_state("workflow-456")
        assert state["status"] == "SUCCEEDED"

    def test_replay_with_custom_handler(
        self,
        replayer: EventReplayer,
        workflow_events: list[Event],
    ) -> None:
        """Test replaying events through a custom handler."""
        processed: list[Event] = []

        replayer.replay_workflow_from_checkpoint(
            "workflow-123",
            checkpoint_sequence=0,
            event_handler=lambda e: processed.append(e),
        )

        assert len(processed) == 7  # All events

    def test_get_entity_history(
        self,
        replayer: EventReplayer,
        workflow_events: list[Event],
    ) -> None:
        """Test getting entity history."""
        history = replayer.get_entity_history(EntityType.STAGE, "stage-1")

        assert len(history) == 2
        assert history[0]["event_type"] == "stage.started"
        assert history[1]["event_type"] == "stage.completed"

    def test_compare_states(
        self,
        replayer: EventReplayer,
        workflow_events: list[Event],
    ) -> None:
        """Test comparing states at different sequences."""
        # Compare between started and completed
        comparison = replayer.compare_states(
            "workflow-123",
            sequence1=workflow_events[1].sequence,  # After STARTED
            sequence2=workflow_events[-1].sequence,  # After COMPLETED
        )

        assert comparison["before"]["status"] == "RUNNING"
        assert comparison["after"]["status"] == "SUCCEEDED"


class TestWorkflowState:
    """Tests for WorkflowState."""

    def test_to_dict(self) -> None:
        """Test serialization."""
        state = WorkflowState(
            workflow_id="w1",
            status="RUNNING",
            application="test-app",
            name="test-workflow",
            context={"key": "value"},
        )

        data = state.to_dict()

        assert data["workflow_id"] == "w1"
        assert data["status"] == "RUNNING"
        assert data["application"] == "test-app"
        assert data["context"]["key"] == "value"

    def test_default_values(self) -> None:
        """Test default values."""
        state = WorkflowState(workflow_id="w1")

        assert state.status is None
        assert state.context == {}
        assert state.stages == {}
        assert state.tasks == {}
