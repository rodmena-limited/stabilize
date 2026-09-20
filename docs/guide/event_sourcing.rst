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
``stage.suspended``        Stage is waiting for a signal (WCP-23/24)
``stage.resumed``          A signal released a suspended stage
=========================  ==========================

A ``stage.suspended`` event is what makes a human-approval wait legible: without
it a multi-day wait is an unexplained silence between ``task.started`` and
``task.completed``, and a replay shows the stage RUNNING. The matching
``stage.resumed`` carries the signal name, and its ``metadata.actor`` is whoever
sent the signal — see :doc:`agentic`.

**Task events:**

=========================  ==========================
Event Type                 Description
=========================  ==========================
``task.started``           Task execution started
``task.completed``         Task finished successfully
``task.failed``            Task failed
``task.retried``           Task is being retried
=========================  ==========================

**Routing events:**

=========================  ==========================
Event Type                 Description
=========================  ==========================
``jump.executed``          A jump moved control to another stage
=========================  ==========================

``jump.executed`` carries ``from_stage_id``, ``to_stage_id`` and a ``jump_type``
of ``self_loop``, ``backward``, ``forward`` or ``restart``. It is what makes a
loop or a retry visible: without it a workflow that looped forty times replays
as though it ran once.

**Not yet emitted**

These types exist and replay understands them, but nothing in the engine
currently records them. They are listed here so their absence from a stream is
not mistaken for a gap in your workflow:

=========================  ==========================
Event Type                 Status
=========================  ==========================
``workflow.paused``        Not emitted — pause is a store call with no recorder
``workflow.resumed``       Not emitted
``status.changed``         Not emitted — needs buffering to stay transactional
``context.updated``        Not emitted
``outputs.updated``        Not emitted
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

Schema Evolution (Upcasting)
----------------------------

As your application evolves, the shape of event payloads may change. Stabilize
can **upcast** historical events to the current schema version during replay, so
old events are interpreted correctly by current code.

Register migrations on the global event migrator. They are applied automatically
(and leniently) on the replay path:

.. code-block:: python

   from stabilize.events import get_event_migrator, Event

   migrator = get_event_migrator()

   @migrator.register(from_version=1, to_version=2)
   def _v1_to_v2(event: Event) -> Event:
       data = dict(event.data)
       data["status"] = data.pop("legacy_status", None)  # rename a field
       return Event(
           event_id=event.event_id,
           event_type=event.event_type,
           timestamp=event.timestamp,
           sequence=event.sequence,
           entity_type=event.entity_type,
           entity_id=event.entity_id,
           workflow_id=event.workflow_id,
           version=event.version,
           data=data,
           metadata=event.metadata,
           schema_version=2,
       )

Behavior:

*  **No migrations registered → no-op.** Replay is byte-for-byte unchanged (this
   is the default).
*  **Lenient on read.** If a step in the migration chain is missing, replay does
   not raise — the event is applied as-is. (The explicit ``migrator.migrate(...)``
   API is strict by default for tooling that wants to fail loudly.)
*  Upcasting targets ``CURRENT_SCHEMA_VERSION``; bump it when you introduce a new
   event shape and register the matching migration.

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

.. note::

   The engine does not create snapshots for you. The replayer will use a
   snapshot if one exists, but nothing currently writes them, so replay reads
   the full event stream unless your application calls
   ``SnapshotStore.create_workflow_snapshot`` itself.


Guarantees
----------

**Reading an event a newer build wrote.** An ``event_type`` this build does not
recognise resolves to ``EventType.UNKNOWN`` with the original string preserved in
``event.data["_raw_event_type"]``, and replay ignores it. An event whose
``schema_version`` is newer than this build's is refused under strict migration
and skipped during replay, rather than being applied with the wrong field
layout.

This matters for rolling deploys: before this, a single unrecognised row raised
inside row-to-event conversion and failed the **entire** query — replay,
``WorkflowStream`` and every durable subscription over that store, not just that
one event.

**Delivery order on PostgreSQL.** ``events.sequence`` is a ``BIGSERIAL``,
assigned at INSERT and not at COMMIT, so a transaction that inserted an earlier
sequence can commit after a later one. A cursor that advanced by sequence would
step over it permanently. Durable subscriptions therefore track a commit
watermark rather than a sequence, and deliver in commit order.

The cost, stated plainly: delivery is held behind the oldest in-flight write
transaction on the database, so one long workflow transaction delays subscription
delivery. Requires PostgreSQL 13 or newer; below that the engine keeps the
sequence cursor and logs a warning naming the loss mode. SQLite is unaffected —
its write lock has always made commit order equal sequence order.

**Transactionality.** Events are appended inside the same transaction that
commits the state they describe, so a rollback cannot leave a phantom event
behind. The exception is an event store on a *different* database from the
workflow store, where no shared transaction exists.
