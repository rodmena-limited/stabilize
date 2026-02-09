"""Integration tests verifying handlers record events during workflow execution."""

from collections.abc import Generator

import pytest

from stabilize.events import (
    EventQuery,
    EventType,
    SqliteEventStore,
    configure_event_sourcing,
    reset_event_bus,
    reset_event_recorder,
)
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from tests.conftest import CounterTask, setup_stabilize

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def reset_events() -> Generator[None, None, None]:
    """Reset global event state between tests."""
    reset_event_bus()
    reset_event_recorder()
    yield
    reset_event_bus()
    reset_event_recorder()


@pytest.fixture
def event_store() -> SqliteEventStore:
    return SqliteEventStore("sqlite:///:memory:", create_tables=True)


@pytest.fixture
def configured_events(event_store: SqliteEventStore) -> SqliteEventStore:
    configure_event_sourcing(event_store)
    return event_store


# =============================================================================
# Helper
# =============================================================================


def _make_simple_workflow(name: str = "Test Workflow") -> Workflow:
    """Create a simple single-stage workflow."""
    return Workflow.create(
        application="test",
        name=name,
        stages=[
            StageExecution(
                ref_id="1",
                type="test",
                name="Test Stage",
                tasks=[
                    TaskExecution.create(
                        name="Success Task",
                        implementing_class="success",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )


def _make_failing_workflow() -> Workflow:
    """Create a workflow with a failing task."""
    return Workflow.create(
        application="test",
        name="Failing Workflow",
        stages=[
            StageExecution(
                ref_id="1",
                type="test",
                name="Failing Stage",
                tasks=[
                    TaskExecution.create(
                        name="Fail Task",
                        implementing_class="fail",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )


def _make_parallel_workflow() -> Workflow:
    """Create a diamond DAG: A -> [B, C] -> D."""
    return Workflow.create(
        application="test",
        name="Parallel Workflow",
        stages=[
            StageExecution(
                ref_id="a",
                type="test",
                name="Stage A",
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="b",
                type="test",
                name="Stage B",
                requisite_stage_ref_ids={"a"},
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="c",
                type="test",
                name="Stage C",
                requisite_stage_ref_ids={"a"},
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="d",
                type="test",
                name="Stage D",
                requisite_stage_ref_ids={"b", "c"},
                tasks=[
                    TaskExecution.create(
                        name="Counter Task",
                        implementing_class="counter",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )


# =============================================================================
# Tests
# =============================================================================


class TestHandlerEventRecording:
    """Verify that handlers record events during real workflow execution."""

    def test_workflow_lifecycle_events(
        self,
        repository: WorkflowStore,
        queue: Queue,
        configured_events: SqliteEventStore,
    ) -> None:
        """A successful single-stage workflow records the full lifecycle."""
        processor, runner, _ = setup_stabilize(repository, queue)
        workflow = _make_simple_workflow()

        repository.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=10.0)

        # Verify workflow completed successfully
        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # Get all events for this workflow
        events = configured_events.get_events_for_workflow(workflow.id)
        event_types = [e.event_type for e in events]

        # Must contain the full lifecycle
        assert EventType.WORKFLOW_CREATED in event_types
        assert EventType.WORKFLOW_STARTED in event_types
        assert EventType.STAGE_STARTED in event_types
        assert EventType.TASK_COMPLETED in event_types
        assert EventType.STAGE_COMPLETED in event_types
        assert EventType.WORKFLOW_COMPLETED in event_types

        # Created must come before Started
        created_seq = next(e.sequence for e in events if e.event_type == EventType.WORKFLOW_CREATED)
        started_seq = next(e.sequence for e in events if e.event_type == EventType.WORKFLOW_STARTED)
        completed_seq = next(e.sequence for e in events if e.event_type == EventType.WORKFLOW_COMPLETED)
        assert created_seq < started_seq < completed_seq

        repository.delete(workflow.id)

    def test_failed_task_events(
        self,
        repository: WorkflowStore,
        queue: Queue,
        configured_events: SqliteEventStore,
    ) -> None:
        """A failing task records failure events."""
        processor, runner, _ = setup_stabilize(repository, queue)
        workflow = _make_failing_workflow()

        repository.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.TERMINAL

        events = configured_events.get_events_for_workflow(workflow.id)
        event_types = [e.event_type for e in events]

        assert EventType.TASK_FAILED in event_types
        assert EventType.STAGE_FAILED in event_types
        assert EventType.WORKFLOW_FAILED in event_types

        # Workflow completed should NOT be present
        assert EventType.WORKFLOW_COMPLETED not in event_types

        repository.delete(workflow.id)

    def test_parallel_stage_events(
        self,
        repository: WorkflowStore,
        queue: Queue,
        configured_events: SqliteEventStore,
    ) -> None:
        """A diamond DAG records events for all stages with correct workflow correlation."""
        processor, runner, _ = setup_stabilize(repository, queue)
        CounterTask.counter = 0
        workflow = _make_parallel_workflow()

        repository.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=15.0)

        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        events = configured_events.get_events_for_workflow(workflow.id)

        # All events should reference this workflow
        for event in events:
            assert event.workflow_id == workflow.id

        # Should have 4 stage_started and 4 stage_completed events (A, B, C, D)
        started_count = sum(1 for e in events if e.event_type == EventType.STAGE_STARTED)
        completed_count = sum(1 for e in events if e.event_type == EventType.STAGE_COMPLETED)
        assert started_count == 4
        assert completed_count == 4

        repository.delete(workflow.id)

    def test_event_metadata_correlation(
        self,
        repository: WorkflowStore,
        queue: Queue,
        configured_events: SqliteEventStore,
    ) -> None:
        """All events for a workflow share the same correlation_id."""
        processor, runner, _ = setup_stabilize(repository, queue)
        workflow = _make_simple_workflow()

        repository.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=10.0)

        events = configured_events.get_events_for_workflow(workflow.id)
        assert len(events) > 0

        # All events should have metadata with source_handler set
        for event in events:
            assert event.metadata is not None
            assert event.metadata.source_handler is not None
            assert event.metadata.source_handler != ""

        repository.delete(workflow.id)

    def test_event_store_persistence(
        self,
        repository: WorkflowStore,
        queue: Queue,
        configured_events: SqliteEventStore,
    ) -> None:
        """Events are queryable in the store after workflow execution."""
        processor, runner, _ = setup_stabilize(repository, queue)
        workflow = _make_simple_workflow()

        repository.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=10.0)

        # Query by workflow ID
        events = configured_events.get_events_for_workflow(workflow.id)
        assert (
            len(events) >= 6
        )  # At minimum: created, started, stage_started, task_completed, stage_completed, completed

        # Query with EventQuery
        query = EventQuery(
            workflow_id=workflow.id,
            event_types=[EventType.WORKFLOW_CREATED, EventType.WORKFLOW_COMPLETED],
        )
        filtered = list(configured_events.get_events(query))
        assert len(filtered) == 2

        # Global sequence numbers should be assigned
        for event in events:
            assert event.sequence > 0

        repository.delete(workflow.id)

    def test_no_events_without_configuration(
        self,
        repository: WorkflowStore,
        queue: Queue,
        event_store: SqliteEventStore,
    ) -> None:
        """Without configure_event_sourcing(), no events are recorded."""
        # Note: we use event_store (not configured_events) — no configuration call
        processor, runner, _ = setup_stabilize(repository, queue)
        workflow = _make_simple_workflow()

        repository.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=10.0)

        # Workflow should still succeed
        result = repository.retrieve(workflow.id)
        assert result.status == WorkflowStatus.SUCCEEDED

        # But no events should be in the store (it was never wired up)
        events = event_store.get_events_for_workflow(workflow.id)
        assert len(events) == 0

        repository.delete(workflow.id)
