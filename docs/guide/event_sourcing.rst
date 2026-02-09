Event Sourcing
==============

Stabilize includes a built-in event sourcing system that records every state
transition as an immutable event. This enables full audit trails, time-travel
debugging, and analytics projections.

Overview
--------

When enabled, all handlers automatically record events as they process messages.
Events are appended to an event store and published to an in-process event bus
for real-time subscriptions.

Key capabilities:

*  **Audit trail**: Every workflow, stage, and task transition is recorded.
*  **Event replay**: Reconstruct workflow state at any point in time.
*  **Projections**: Build metrics, timelines, and custom views from events.
*  **Subscriptions**: React to events in real-time (logging, webhooks, etc.).
*  **Snapshots**: Speed up replay for long-running workflows.

Quick Setup
-----------

Enable event sourcing with a single call:

.. code-block:: python

   from stabilize.events import configure_event_sourcing, SqliteEventStore

   event_store = SqliteEventStore("sqlite:///events.db", create_tables=True)
   configure_event_sourcing(event_store)

   # That's it — all handlers now record events automatically.

The ``configure_event_sourcing`` function sets up a global event recorder and
event bus. Handlers detect the recorder via a global fallback, so no changes
to existing workflow code are needed.

Event Types
-----------

Events are organized by entity lifecycle:

**Workflow events:**

=========================  ==========================
Event Type                 Description
=========================  ==========================
``workflow.created``       Workflow was created
``workflow.started``       Workflow execution started
``workflow.completed``     Workflow finished successfully
``workflow.failed``        Workflow failed
``workflow.canceled``      Workflow was canceled
``workflow.paused``        Workflow was paused
``workflow.resumed``       Workflow was resumed
=========================  ==========================

**Stage events:**

=========================  ==========================
Event Type                 Description
=========================  ==========================
``stage.started``          Stage execution started
``stage.completed``        Stage finished successfully
``stage.failed``           Stage failed
``stage.skipped``          Stage was skipped
``stage.canceled``         Stage was canceled
=========================  ==========================

**Task events:**

=========================  ==========================
Event Type                 Description
=========================  ==========================
``task.started``           Task execution started
``task.completed``         Task finished successfully
``task.failed``            Task failed
``task.retried``           Task is being retried
=========================  ==========================

**State change events:**

=========================  ==========================
Event Type                 Description
=========================  ==========================
``status.changed``         Generic status change
``context.updated``        Stage context was updated
``outputs.updated``        Stage outputs were updated
``jump.executed``          Dynamic jump was executed
=========================  ==========================

Subscribing to Events
---------------------

Use the event bus to receive events in real-time:

.. code-block:: python

   from stabilize.events import get_event_bus, EventType

   bus = get_event_bus()

   # Subscribe to all events
   bus.subscribe("logger", lambda e: print(f"{e.event_type.value}: {e.entity_id}"))

   # Subscribe to specific event types
   bus.subscribe(
       "failure-alert",
       lambda e: send_alert(e),
       event_types={EventType.WORKFLOW_FAILED, EventType.TASK_FAILED},
   )

   # Filter by workflow
   bus.subscribe(
       "workflow-monitor",
       lambda e: track(e),
       workflow_filter="my-workflow-id",
   )

Projections
-----------

Projections build read-only views from events. Stabilize includes two built-in
projections:

**WorkflowTimelineProjection** — builds a human-readable execution timeline:

.. code-block:: python

   from stabilize.events import WorkflowTimelineProjection

   timeline_proj = WorkflowTimelineProjection(workflow.id)

   # Apply events (from store or via bus subscription)
   for event in event_store.get_events_for_workflow(workflow.id):
       timeline_proj.apply(event)

   timeline = timeline_proj.get_state()
   print(f"Duration: {timeline.total_duration_ms}ms")
   print(f"Status: {timeline.status}")

   for entry in timeline_proj.get_stages():
       print(f"  {entry.event_type}: {entry.entity_name} ({entry.duration_ms}ms)")

**StageMetricsProjection** — aggregates execution metrics:

.. code-block:: python

   from stabilize.events import StageMetricsProjection

   metrics = StageMetricsProjection()

   # Subscribe to the bus for real-time metrics
   bus.subscribe("metrics", metrics.apply)

   # After workflows run, query metrics
   for stage_type, m in metrics.get_state().items():
       print(f"{stage_type}: {m.execution_count} runs, {m.success_rate:.0f}% success")

Event Replay
------------

The ``EventReplayer`` reconstructs workflow state from events:

.. code-block:: python

   from stabilize.events import EventReplayer

   replayer = EventReplayer(event_store)

   # Rebuild current state
   state = replayer.rebuild_workflow_state(workflow.id)
   print(state["status"])
   print(state["stages"])

   # Time-travel: state at a specific sequence number
   partial = replayer.rebuild_workflow_state(workflow.id, as_of_sequence=50)

   # Time-travel: state at a specific point in time
   from datetime import datetime, UTC
   historical = replayer.time_travel_query(workflow.id, as_of_time=some_datetime)

Event Stores
------------

Three event store backends are available:

**SQLite** (development, testing, and single-node production):

.. code-block:: python

   from stabilize.events import SqliteEventStore
   store = SqliteEventStore("sqlite:///events.db", create_tables=True)

**PostgreSQL** (production, requires ``stabilize[postgres]``):

.. code-block:: python

   from stabilize.events import PostgresEventStore
   store = PostgresEventStore("postgresql://user:pass@host/db")

Snapshots
---------

For long-running workflows with many events, snapshots speed up replay by
providing periodic checkpoints:

.. code-block:: python

   from stabilize.events import SnapshotPolicy, SnapshotStore

   # Snapshot every 100 events
   policy = SnapshotPolicy(every_n_events=100)

   # The replayer uses snapshots automatically when available
   replayer = EventReplayer(event_store, snapshot_store=snapshot_store)
   state = replayer.rebuild_workflow_state(workflow.id)  # Starts from latest snapshot
