#!/usr/bin/env python3
"""
Event Sourcing Example - Demonstrates Stabilize's event sourcing capabilities.

This example shows how to:
1. Enable event sourcing with one line
2. Subscribe to events for real-time logging
3. Use projections for metrics and timelines
4. Replay events to reconstruct workflow state

Run with:
    python examples/event-sourcing-example.py
"""

import logging

logging.basicConfig(level=logging.ERROR)

from stabilize import (
    Orchestrator,
    PythonTask,
    Queue,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStore,
)
from stabilize.events import (
    EventReplayer,
    EventType,
    SqliteEventStore,
    StageMetricsProjection,
    WorkflowTimelineProjection,
    configure_event_sourcing,
    get_event_bus,
    reset_event_bus,
    reset_event_recorder,
)

# =============================================================================
# Helper: Setup pipeline infrastructure with event sourcing
# =============================================================================


def setup_pipeline_runner(
    store: WorkflowStore, queue: Queue, event_store: SqliteEventStore
) -> tuple[QueueProcessor, Orchestrator]:
    """Create processor and orchestrator with event sourcing enabled."""
    task_registry = TaskRegistry()
    task_registry.register("python", PythonTask)

    # Enable event sourcing — handlers automatically record events
    configure_event_sourcing(event_store)

    processor = QueueProcessor(queue, store=store, task_registry=task_registry)
    orchestrator = Orchestrator(queue)
    return processor, orchestrator


# =============================================================================
# Example 1: Basic Event Logging
# =============================================================================


def example_event_logging() -> None:
    """Enable event sourcing and log all events during workflow execution."""
    reset_event_bus()
    reset_event_recorder()

    print("\n" + "=" * 60)
    print("Example 1: Basic Event Logging")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    event_store = SqliteEventStore("sqlite:///:memory:", create_tables=True)
    processor, orchestrator = setup_pipeline_runner(store, queue, event_store)

    # Subscribe to events for real-time logging
    bus = get_event_bus()
    bus.subscribe(
        "logger",
        lambda e: print(f"  [{e.event_type.value:>20s}] {e.entity_type.value}={e.entity_id[:8]}..."),
    )

    workflow = Workflow.create(
        application="event-example",
        name="Logged Workflow",
        stages=[
            StageExecution(
                ref_id="1",
                type="python",
                name="Compute",
                context={"script": "RESULT = {'answer': 42}\nprint('Computing...')"},
                tasks=[
                    TaskExecution.create(
                        name="Run Python",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Query recorded events
    events = event_store.get_events_for_workflow(workflow.id)
    print(f"\nTotal events recorded: {len(events)}")
    print("Event types:")
    for event in events:
        print(f"  seq={event.sequence:>3d}  {event.event_type.value}")


# =============================================================================
# Example 2: Projections — Metrics and Timeline
# =============================================================================


def example_projections() -> None:
    """Use projections to build metrics and timelines from events."""
    reset_event_bus()
    reset_event_recorder()

    print("\n" + "=" * 60)
    print("Example 2: Projections — Metrics & Timeline")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    event_store = SqliteEventStore("sqlite:///:memory:", create_tables=True)
    processor, orchestrator = setup_pipeline_runner(store, queue, event_store)

    # Set up projections
    metrics = StageMetricsProjection()
    bus = get_event_bus()
    bus.subscribe("metrics", metrics.apply)

    #    Generate
    #    /      \
    #  Analyze  Transform
    #    \      /
    #    Report

    workflow = Workflow.create(
        application="event-example",
        name="Analytics Pipeline",
        stages=[
            StageExecution(
                ref_id="generate",
                type="python",
                name="Generate Data",
                context={
                    "script": """
import random
random.seed(42)
RESULT = [random.randint(1, 100) for _ in range(50)]
print(f"Generated {len(RESULT)} numbers")
""",
                },
                tasks=[
                    TaskExecution.create(
                        name="Generate",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="analyze",
                type="python",
                name="Analyze",
                requisite_stage_ref_ids={"generate"},
                context={
                    "script": """
data = INPUT.get('result', [])
RESULT = {'mean': sum(data)/len(data), 'count': len(data)}
print(f"Analyzed {len(data)} items")
""",
                },
                tasks=[
                    TaskExecution.create(
                        name="Analyze",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="transform",
                type="python",
                name="Transform",
                requisite_stage_ref_ids={"generate"},
                context={
                    "script": """
data = INPUT.get('result', [])
RESULT = [x * 2 for x in data]
print(f"Transformed {len(RESULT)} items")
""",
                },
                tasks=[
                    TaskExecution.create(
                        name="Transform",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="report",
                type="python",
                name="Report",
                requisite_stage_ref_ids={"analyze", "transform"},
                context={
                    "script": "RESULT = {'status': 'complete'}\nprint('Report generated')",
                },
                tasks=[
                    TaskExecution.create(
                        name="Report",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Build timeline from recorded events
    timeline_projection = WorkflowTimelineProjection(workflow.id)
    for event in event_store.get_events_for_workflow(workflow.id):
        timeline_projection.apply(event)
    timeline = timeline_projection.get_state()

    print(f"\nWorkflow Timeline ({len(timeline.entries)} entries):")
    if timeline.start_time:
        print(f"  Started:  {timeline.start_time.strftime('%H:%M:%S.%f')}")
    if timeline.end_time:
        print(f"  Ended:    {timeline.end_time.strftime('%H:%M:%S.%f')}")
    if timeline.total_duration_ms is not None:
        print(f"  Duration: {timeline.total_duration_ms}ms")

    print("\nStage entries:")
    for entry in timeline_projection.get_stages():
        duration = f" ({entry.duration_ms}ms)" if entry.duration_ms else ""
        print(f"  {entry.event_type:>18s} | {entry.entity_name or entry.entity_id[:8]}{duration}")

    # Show stage metrics
    print("\nStage Metrics:")
    for stage_type, stage_metrics in metrics.get_state().items():
        print(
            f"  {stage_type}: "
            f"executions={stage_metrics.execution_count}, "
            f"success_rate={stage_metrics.success_rate:.0f}%"
        )


# =============================================================================
# Example 3: Event Replay — State Reconstruction
# =============================================================================


def example_event_replay() -> None:
    """Replay events to reconstruct workflow state."""
    reset_event_bus()
    reset_event_recorder()

    print("\n" + "=" * 60)
    print("Example 3: Event Replay — State Reconstruction")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    event_store = SqliteEventStore("sqlite:///:memory:", create_tables=True)
    processor, orchestrator = setup_pipeline_runner(store, queue, event_store)

    workflow = Workflow.create(
        application="event-example",
        name="Replayable Workflow",
        stages=[
            StageExecution(
                ref_id="1",
                type="python",
                name="Step 1",
                context={"script": "RESULT = {'step': 1, 'data': 'hello'}\nprint('Step 1 done')"},
                tasks=[
                    TaskExecution.create(
                        name="Run Step 1",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="2",
                type="python",
                name="Step 2",
                requisite_stage_ref_ids={"1"},
                context={"script": "RESULT = {'step': 2, 'data': 'world'}\nprint('Step 2 done')"},
                tasks=[
                    TaskExecution.create(
                        name="Run Step 2",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Replay events to reconstruct state
    replayer = EventReplayer(event_store)
    state = replayer.rebuild_workflow_state(workflow.id)

    print(f"\nReconstructed state from {len(event_store.get_events_for_workflow(workflow.id))} events:")
    print(f"  Workflow: {state.get('name')}")
    print(f"  Status:   {state.get('status')}")
    print(f"  Stages:   {len(state.get('stages', {}))}")

    # Compare with actual DB state
    db_result = store.retrieve(workflow.id)
    print(f"\nDB state matches: status={db_result.status.value}, stages={len(db_result.stages)}")

    # Time travel: see state partway through execution
    events = event_store.get_events_for_workflow(workflow.id)
    if len(events) > 3:
        mid_sequence = events[len(events) // 2].sequence
        partial_state = replayer.rebuild_workflow_state(workflow.id, as_of_sequence=mid_sequence)
        print(f"\nTime-travel to sequence {mid_sequence}:")
        print(f"  Status at that point: {partial_state.get('status')}")
        print(f"  Stages at that point: {len(partial_state.get('stages', {}))}")


# =============================================================================
# Example 4: Custom Event Subscriptions
# =============================================================================


def example_custom_subscriptions() -> None:
    """Subscribe to specific event types for targeted monitoring."""
    reset_event_bus()
    reset_event_recorder()

    print("\n" + "=" * 60)
    print("Example 4: Custom Event Subscriptions")
    print("=" * 60)

    store = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True)
    queue = SqliteQueue("sqlite:///:memory:", table_name="queue_messages")
    queue._create_table()
    event_store = SqliteEventStore("sqlite:///:memory:", create_tables=True)
    processor, orchestrator = setup_pipeline_runner(store, queue, event_store)

    # Track only stage completions
    completed_stages: list[str] = []
    bus = get_event_bus()
    bus.subscribe(
        "stage-tracker",
        lambda e: completed_stages.append(e.data.get("name", "unknown")),
        event_types={EventType.STAGE_COMPLETED},
    )

    # Track workflow lifecycle
    lifecycle: list[str] = []
    bus.subscribe(
        "lifecycle-tracker",
        lambda e: lifecycle.append(e.event_type.value),
        event_types={
            EventType.WORKFLOW_CREATED,
            EventType.WORKFLOW_STARTED,
            EventType.WORKFLOW_COMPLETED,
        },
    )

    workflow = Workflow.create(
        application="event-example",
        name="Monitored Workflow",
        stages=[
            StageExecution(
                ref_id="a",
                type="python",
                name="Alpha",
                context={"script": "RESULT = {'stage': 'alpha'}"},
                tasks=[
                    TaskExecution.create(
                        name="Run",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="b",
                type="python",
                name="Beta",
                requisite_stage_ref_ids={"a"},
                context={"script": "RESULT = {'stage': 'beta'}"},
                tasks=[
                    TaskExecution.create(
                        name="Run",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
            StageExecution(
                ref_id="c",
                type="python",
                name="Gamma",
                requisite_stage_ref_ids={"b"},
                context={"script": "RESULT = {'stage': 'gamma'}"},
                tasks=[
                    TaskExecution.create(
                        name="Run",
                        implementing_class="python",
                        stage_start=True,
                        stage_end=True,
                    ),
                ],
            ),
        ],
    )

    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)

    # Show what each subscription captured
    print(f"\nCompleted stages (from subscription): {completed_stages}")
    print(f"Workflow lifecycle (from subscription): {lifecycle}")

    # Show bus statistics
    stats = bus.stats
    print("\nEvent bus stats:")
    print(f"  Events published: {stats.events_published}")
    print(f"  Events delivered: {stats.events_delivered}")
    print(f"  Active subscriptions: {stats.subscriptions_active}")


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    print("Stabilize Event Sourcing Examples")
    print("=" * 60)

    example_event_logging()
    example_projections()
    example_event_replay()
    example_custom_subscriptions()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
