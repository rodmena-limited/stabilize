Stability and delivery guarantees
=================================

Public API
----------

The public API of stabilize, as of 0.33.0 and frozen at 1.0.0, is:

* every name in ``stabilize.__all__``;
* the message types in ``stabilize.queue.messages`` and their fields;
* the persisted schema created by ``stabilize mg-up`` (PostgreSQL) and by
  ``SqliteWorkflowStore(create_tables=True)`` (SQLite), including the engine
  context keys enumerated in ``stabilize.models.stage.engine_keys``;
* the ``stabilize`` command-line interface: commands, options and exit codes.

Everything else, including handler classes and modules under
``stabilize.handlers`` and ``stabilize.persistence``, is internal and can change
in a minor release.

Versioning
----------

stabilize will follow Semantic Versioning 2.0.0 from 1.0.0, which promotes the latest 0.33.x
unchanged once it has run in production:

* a public name, message type, persisted column or CLI command is not removed
  or changed incompatibly before 2.0.0;
* a deprecation warns for at least one minor release before removal in a
  major release;
* every migration is forward-only and additive within 1.x (already true from 0.33.0). ``mg-up``
  upgrades a 0.33.1 database in place with in-flight work resumed (exercised in CI by
  ``audit/evaluations/upgrade_inflight.py``), and hands databases built by 0.32.1 and 0.33.1
  over to migretti (``tests/test_mg_up_takeover.py``).

Delivery guarantees
-------------------

These are what the test suite and the ``audit/evaluations`` probes exercise;
they are stated as measured, not as aspirations.

* **Messages are delivered at least once and handled effectively once.** A
  handled message is recorded in ``processed_messages`` in the same transaction
  as its effects; a redelivered message whose record exists is acknowledged
  without being handled again. Records older than
  ``processed_messages_max_age_hours`` are removed by the retention sweep.
* **A task's side effects can repeat in three situations**, and only these:
  a worker's lock renewal fails for longer than the queue's ``lock_duration``
  (another worker then takes the message; enable the task lease to exclude
  this); a worker dies after the task's side effect and before its result
  commits; a task returns ``RUNNING`` and is polled again, by design.
* **Task hooks** (``on_timeout``, ``on_cancel``, ``on_cleanup``) and stage
  finalizers run once per committed transition, after the commit: a process
  that dies between the commit and the hook does not run it (at most once).
  ``on_cancel``'s returned context and outputs are saved in a later
  transaction, so the workflow can complete before they are visible.
* **A failed task ends its stage**: no later task of the stage starts after a
  task completes TERMINAL, STOPPED or CANCELED, or FAILED_CONTINUE converted by
  ``continuePipelineOnFailure``. A task returning ``TaskResult.failed_continue``
  itself lets the stage continue.
* **Recovery** re-issues, for one lost message, exactly the message the normal
  path would have pushed, rebuilt from committed state; it never re-runs a task
  whose result was committed.
* **Recovery window**: recovery examines only workflows whose ``start_time`` is
  within ``recovery_max_age_hours`` (24 h by default) or not yet set. A RUNNING
  workflow that started earlier and lost a message is not recovered, however
  recent its last stage or task activity; raise ``recovery_max_age_hours`` (or
  pass ``max_age_hours`` to ``recover_on_startup``) to cover workflows that run
  longer than the window. Probed in both directions by
  ``tests/test_recovery_window.py``.
* **Retention keeps live dedup records**: the processor's retention sweep and
  ``stabilize prune-processed`` delete a ``processed_messages`` record only when
  its message is no longer in its queue table, so a message whose handler
  committed but whose ack was lost is never handled again after retention.
  ``prune-processed`` reads the queue tables from ``--db-url`` unless
  ``--queue-db-url`` is given; when the queue is in another database it must be
  given.
* **Event purge keeps subscriptions whole**: ``stabilize purge-events`` and
  ``EventStore.purge_events`` refuse while an enabled durable subscription still
  has undelivered events inside the bound. Pass ``--ignore-subscriptions`` to
  purge anyway.

Several queue tables in one store
---------------------------------

Processors may use different queue tables (``table_name``) on one store, each
with its own task registry. Then:

* start workflows through ``Orchestrator(queue).start()`` with the queue the
  workflow belongs to, so its ``queue_table`` is recorded before any recovery
  sweep can see it;
* each processor recovers only the workflows whose ``queue_table`` is its own;
  rows written before 0.33.0 have no ``queue_table`` and belong to
  ``queue_messages``;
* processed-message keys of a table other than ``queue_messages`` are
  ``"<table>:<row id>"``; records written before 0.33.0 for such a table under
  the bare row id stop matching after the upgrade, so a message of that table
  in flight across the upgrade can be handled once more;
* on PostgreSQL, the migrations index only ``queue_messages`` and
  ``queue_messages_dlq``. For each further queue table, create the index that
  serves recovery and ``purge-workflows``; without it each lookup scans the
  table::

      CREATE INDEX IF NOT EXISTS idx_<table>_execution_id
          ON <table> ((payload ->> 'execution_id'));
      CREATE INDEX IF NOT EXISTS idx_<table>_dlq_execution_id
          ON <table>_dlq ((payload ->> 'execution_id'));

  SQLite queue tables get these indexes when the queue creates its tables.
