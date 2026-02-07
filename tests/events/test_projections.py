"""Tests for event projections."""

from datetime import UTC, datetime, timedelta

import pytest

from stabilize.events.base import (
    EntityType,
    Event,
    EventMetadata,
    EventType,
)
from stabilize.events.projections import (
    StageMetricsProjection,
    WorkflowTimelineProjection,
)


class TestWorkflowTimelineProjection:
    """Tests for WorkflowTimelineProjection."""

    @pytest.fixture
    def projection(self) -> WorkflowTimelineProjection:
        """Create a timeline projection."""
        return WorkflowTimelineProjection("workflow-123")

    @pytest.fixture
    def sample_events(self) -> list[Event]:
        """Create sample events for a workflow."""
        now = datetime.now(UTC)
        metadata = EventMetadata(correlation_id="workflow-123")

        return [
            Event(
                event_type=EventType.WORKFLOW_CREATED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-123",
                workflow_id="workflow-123",
                timestamp=now,
                data={"application": "test-app", "type": "PIPELINE"},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.WORKFLOW_STARTED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-123",
                workflow_id="workflow-123",
                timestamp=now + timedelta(seconds=1),
                data={},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.STAGE_STARTED,
                entity_type=EntityType.STAGE,
                entity_id="stage-1",
                workflow_id="workflow-123",
                timestamp=now + timedelta(seconds=2),
                data={"name": "build", "type": "shell"},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                entity_id="stage-1",
                workflow_id="workflow-123",
                timestamp=now + timedelta(seconds=5),
                data={"name": "build", "status": "SUCCEEDED", "duration_ms": 3000},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.WORKFLOW_COMPLETED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-123",
                workflow_id="workflow-123",
                timestamp=now + timedelta(seconds=6),
                data={"status": "SUCCEEDED"},
                metadata=metadata,
            ),
        ]

    def test_apply_events(
        self,
        projection: WorkflowTimelineProjection,
        sample_events: list[Event],
    ) -> None:
        """Test applying events to the timeline."""
        for event in sample_events:
            projection.apply(event)

        timeline = projection.get_state()

        assert timeline.workflow_id == "workflow-123"
        assert len(timeline.entries) == 5

    def test_timeline_status(
        self,
        projection: WorkflowTimelineProjection,
        sample_events: list[Event],
    ) -> None:
        """Test that timeline captures final status."""
        for event in sample_events:
            projection.apply(event)

        timeline = projection.get_state()

        assert timeline.status == "SUCCEEDED"

    def test_timeline_start_end_times(
        self,
        projection: WorkflowTimelineProjection,
        sample_events: list[Event],
    ) -> None:
        """Test that timeline captures timing."""
        for event in sample_events:
            projection.apply(event)

        timeline = projection.get_state()

        assert timeline.start_time is not None
        assert timeline.end_time is not None
        assert timeline.total_duration_ms is not None
        assert timeline.total_duration_ms > 0

    def test_get_stages(
        self,
        projection: WorkflowTimelineProjection,
        sample_events: list[Event],
    ) -> None:
        """Test filtering stage entries."""
        for event in sample_events:
            projection.apply(event)

        stages = projection.get_stages()

        assert len(stages) == 2
        assert all(e.entity_type == "stage" for e in stages)

    def test_get_failures(
        self,
        projection: WorkflowTimelineProjection,
    ) -> None:
        """Test filtering failure entries."""
        metadata = EventMetadata(correlation_id="workflow-123")
        now = datetime.now(UTC)

        events = [
            Event(
                event_type=EventType.WORKFLOW_STARTED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-123",
                workflow_id="workflow-123",
                timestamp=now,
                metadata=metadata,
            ),
            Event(
                event_type=EventType.STAGE_FAILED,
                entity_type=EntityType.STAGE,
                entity_id="stage-1",
                workflow_id="workflow-123",
                timestamp=now + timedelta(seconds=1),
                data={"error": "Build failed"},
                metadata=metadata,
            ),
            Event(
                event_type=EventType.WORKFLOW_FAILED,
                entity_type=EntityType.WORKFLOW,
                entity_id="workflow-123",
                workflow_id="workflow-123",
                timestamp=now + timedelta(seconds=2),
                data={"status": "TERMINAL"},
                metadata=metadata,
            ),
        ]

        for event in events:
            projection.apply(event)

        failures = projection.get_failures()

        assert len(failures) == 2

    def test_ignores_other_workflows(
        self,
        projection: WorkflowTimelineProjection,
    ) -> None:
        """Test that events for other workflows are ignored."""
        event = Event(
            event_type=EventType.WORKFLOW_CREATED,
            entity_type=EntityType.WORKFLOW,
            entity_id="workflow-456",
            workflow_id="workflow-456",
        )

        projection.apply(event)
        timeline = projection.get_state()

        assert len(timeline.entries) == 0

    def test_reset(
        self,
        projection: WorkflowTimelineProjection,
        sample_events: list[Event],
    ) -> None:
        """Test resetting the projection."""
        for event in sample_events:
            projection.apply(event)

        projection.reset()
        timeline = projection.get_state()

        assert len(timeline.entries) == 0
        assert timeline.status is None

    def test_to_dict(
        self,
        projection: WorkflowTimelineProjection,
        sample_events: list[Event],
    ) -> None:
        """Test serialization."""
        for event in sample_events:
            projection.apply(event)

        data = projection.get_state().to_dict()

        assert data["workflow_id"] == "workflow-123"
        assert "entries" in data
        assert "total_duration_ms" in data


class TestStageMetricsProjection:
    """Tests for StageMetricsProjection."""

    @pytest.fixture
    def projection(self) -> StageMetricsProjection:
        """Create a metrics projection."""
        return StageMetricsProjection()

    def test_track_executions(self, projection: StageMetricsProjection) -> None:
        """Test tracking stage executions."""
        events = [
            Event(
                event_type=EventType.STAGE_STARTED,
                entity_type=EntityType.STAGE,
                entity_id="s1",
                workflow_id="w1",
                data={"type": "shell"},
            ),
            Event(
                event_type=EventType.STAGE_STARTED,
                entity_type=EntityType.STAGE,
                entity_id="s2",
                workflow_id="w1",
                data={"type": "shell"},
            ),
            Event(
                event_type=EventType.STAGE_STARTED,
                entity_type=EntityType.STAGE,
                entity_id="s3",
                workflow_id="w1",
                data={"type": "python"},
            ),
        ]

        for event in events:
            projection.apply(event)

        metrics = projection.get_state()

        assert metrics["shell"].execution_count == 2
        assert metrics["python"].execution_count == 1

    def test_track_success_failure(self, projection: StageMetricsProjection) -> None:
        """Test tracking success and failure counts."""
        events = [
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                entity_id="s1",
                workflow_id="w1",
                data={"type": "shell", "duration_ms": 1000},
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                entity_id="s2",
                workflow_id="w1",
                data={"type": "shell", "duration_ms": 2000},
            ),
            Event(
                event_type=EventType.STAGE_FAILED,
                entity_type=EntityType.STAGE,
                entity_id="s3",
                workflow_id="w1",
                data={"type": "shell", "duration_ms": 500},
            ),
        ]

        for event in events:
            projection.apply(event)

        metrics = projection.get_metrics_for_type("shell")

        assert metrics is not None
        assert metrics.success_count == 2
        assert metrics.failure_count == 1

    def test_success_rate(self, projection: StageMetricsProjection) -> None:
        """Test success rate calculation."""
        events = [
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "shell"},
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "shell"},
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "shell"},
            ),
            Event(
                event_type=EventType.STAGE_FAILED,
                entity_type=EntityType.STAGE,
                data={"type": "shell"},
            ),
        ]

        for event in events:
            projection.apply(event)

        metrics = projection.get_metrics_for_type("shell")
        assert metrics is not None
        assert metrics.success_rate == 75.0

    def test_duration_statistics(self, projection: StageMetricsProjection) -> None:
        """Test duration statistics."""
        events = [
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "shell", "duration_ms": 100},
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "shell", "duration_ms": 200},
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "shell", "duration_ms": 300},
            ),
        ]

        for event in events:
            projection.apply(event)

        metrics = projection.get_metrics_for_type("shell")
        assert metrics is not None
        assert metrics.avg_duration_ms == 200.0
        assert metrics.min_duration_ms == 100
        assert metrics.max_duration_ms == 300

    def test_track_skipped(self, projection: StageMetricsProjection) -> None:
        """Test tracking skipped stages."""
        events = [
            Event(
                event_type=EventType.STAGE_SKIPPED,
                entity_type=EntityType.STAGE,
                data={"type": "shell"},
            ),
            Event(
                event_type=EventType.STAGE_SKIPPED,
                entity_type=EntityType.STAGE,
                data={"type": "shell"},
            ),
        ]

        for event in events:
            projection.apply(event)

        metrics = projection.get_metrics_for_type("shell")
        assert metrics is not None
        assert metrics.skip_count == 2

    def test_track_task_metrics(self, projection: StageMetricsProjection) -> None:
        """Test tracking task metrics."""
        events = [
            Event(
                event_type=EventType.TASK_STARTED,
                entity_type=EntityType.TASK,
                data={"name": "compile"},
            ),
            Event(
                event_type=EventType.TASK_COMPLETED,
                entity_type=EntityType.TASK,
                data={"name": "compile", "duration_ms": 1000},
            ),
            Event(
                event_type=EventType.TASK_RETRIED,
                entity_type=EntityType.TASK,
                data={"name": "deploy"},
            ),
        ]

        for event in events:
            projection.apply(event)

        task_metrics = projection.get_task_state()
        assert "compile" in task_metrics
        assert task_metrics["compile"].success_count == 1
        assert "deploy" in task_metrics
        assert task_metrics["deploy"].retry_count == 1

    def test_top_slowest_stages(self, projection: StageMetricsProjection) -> None:
        """Test getting slowest stages."""
        events = [
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "fast", "duration_ms": 100},
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "medium", "duration_ms": 500},
            ),
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "slow", "duration_ms": 1000},
            ),
        ]

        for event in events:
            projection.apply(event)

        slowest = projection.get_top_slowest_stages(n=2)

        assert len(slowest) == 2
        assert slowest[0][0] == "slow"
        assert slowest[1][0] == "medium"

    def test_reset(self, projection: StageMetricsProjection) -> None:
        """Test resetting metrics."""
        projection.apply(
            Event(
                event_type=EventType.STAGE_STARTED,
                entity_type=EntityType.STAGE,
                data={"type": "shell"},
            )
        )

        projection.reset()

        assert len(projection.get_state()) == 0

    def test_to_dict(self, projection: StageMetricsProjection) -> None:
        """Test serialization."""
        projection.apply(
            Event(
                event_type=EventType.STAGE_COMPLETED,
                entity_type=EntityType.STAGE,
                data={"type": "shell", "duration_ms": 1000},
            )
        )

        data = projection.to_dict()

        assert "stages" in data
        assert "shell" in data["stages"]
        assert data["stages"]["shell"]["success_count"] == 1
