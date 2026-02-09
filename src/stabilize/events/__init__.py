"""
Event sourcing for Stabilize.

This module provides event sourcing capabilities for the Stabilize workflow engine.
Events become the source of truth, enabling:
- Full audit trail of all state changes
- Time-travel debugging (query state at any point)
- Event replay for state reconstruction
- Rich subscriptions for external integrations

Quick Start:
    from stabilize.events import (
        configure_event_sourcing,
        SqliteEventStore,
        get_event_bus,
        EventType,
    )

    # 1. Setup event sourcing
    event_store = SqliteEventStore("sqlite:///app.db", create_tables=True)
    configure_event_sourcing(event_store)

    # 2. Subscribe to events
    bus = get_event_bus()
    bus.subscribe("logger", lambda e: print(f"Event: {e.event_type.value}"))

    # 3. Run workflows - events are recorded automatically

    # 4. Query history
    from stabilize.events import EventReplayer
    replayer = EventReplayer(event_store)
    state = replayer.rebuild_workflow_state(workflow.id)
"""

# Base types
from stabilize.events.base import (
    EntityType,
    Event,
    EventMetadata,
    EventType,
    create_stage_event,
    create_task_event,
    create_workflow_event,
)

# Event bus (in-process pub/sub)
from stabilize.events.bus import (
    EventBus,
    EventBusStats,
    Subscription,
    SubscriptionMode,
    configure_event_bus,
    get_event_bus,
    reset_event_bus,
)

# Projections
from stabilize.events.projections import (
    Projection,
    StageMetrics,
    StageMetricsProjection,
    TimelineEntry,
    WorkflowTimeline,
    WorkflowTimelineProjection,
)

# Event recorder
from stabilize.events.recorder import (
    EventRecorder,
    configure_event_recorder,
    get_event_metadata,
    get_event_recorder,
    reset_event_recorder,
    set_event_context,
)

# Replay
from stabilize.events.replay import EventReplayer, ReplayResult, WorkflowState

# Snapshots
from stabilize.events.snapshots import Snapshot, SnapshotPolicy, SnapshotStore

# Event store
from stabilize.events.store import (
    AppendResult,
    EventQuery,
    EventStore,
    SqliteEventStore,
)

# Subscriptions
from stabilize.events.subscriptions import DurableSubscription, SubscriptionManager

__all__ = [
    # Base types
    "Event",
    "EventType",
    "EntityType",
    "EventMetadata",
    "create_workflow_event",
    "create_stage_event",
    "create_task_event",
    # Event bus
    "EventBus",
    "EventBusStats",
    "Subscription",
    "SubscriptionMode",
    "get_event_bus",
    "reset_event_bus",
    "configure_event_bus",
    # Event recorder
    "EventRecorder",
    "get_event_recorder",
    "configure_event_recorder",
    "reset_event_recorder",
    "set_event_context",
    "get_event_metadata",
    # Event store
    "EventStore",
    "EventQuery",
    "AppendResult",
    "SqliteEventStore",
    # Projections
    "Projection",
    "TimelineEntry",
    "WorkflowTimeline",
    "WorkflowTimelineProjection",
    "StageMetrics",
    "StageMetricsProjection",
    # Replay
    "EventReplayer",
    "ReplayResult",
    "WorkflowState",
    # Snapshots
    "Snapshot",
    "SnapshotStore",
    "SnapshotPolicy",
    # Subscriptions
    "DurableSubscription",
    "SubscriptionManager",
    # Configuration
    "configure_event_sourcing",
]

# PostgreSQL support is optional
try:
    from stabilize.events.store.postgres import PostgresEventStore  # noqa: F401

    __all__.append("PostgresEventStore")
except ImportError:
    pass


def configure_event_sourcing(
    event_store: EventStore,
    publish_to_bus: bool = True,
    bus_max_workers: int = 4,
) -> EventRecorder:
    """
    One-line setup for event sourcing.

    This configures both the event bus and event recorder,
    setting up the global instances for use throughout the application.

    Args:
        event_store: The event store for persisting events.
        publish_to_bus: Whether to publish events to the bus.
        bus_max_workers: Maximum workers for async bus handlers.

    Returns:
        The configured EventRecorder.

    Example:
        from stabilize.events import configure_event_sourcing, SqliteEventStore

        event_store = SqliteEventStore("sqlite:///app.db", create_tables=True)
        recorder = configure_event_sourcing(event_store)
    """
    # Configure the event bus
    configure_event_bus(max_workers=bus_max_workers)

    # Configure the event recorder
    recorder = configure_event_recorder(event_store, publish_to_bus=publish_to_bus)

    return recorder
