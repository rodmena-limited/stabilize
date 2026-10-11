Stability and delivery guarantees (1.0)
=======================================

Public API
----------

The public API of stabilize 1.x is:

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

stabilize follows Semantic Versioning 2.0.0 from 1.0.0:

* a public name, message type, persisted column or CLI command is not removed
  or changed incompatibly before 2.0.0;
* a deprecation warns for at least one minor release before removal in a
  major release;
* every migration is forward-only and additive within 1.x, and ``mg-up``
  upgrades a 0.32.x database in place with in-flight work resumed (exercised
  in CI by ``audit/evaluations/upgrade_inflight.py``).

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
  finalizers run once per committed transition; across a crash between the
  commit and the hook they can run again on redelivery, or not at all if the
  process dies after the commit and before the hook. Hooks must be idempotent.
* **A failed task ends its stage**: no later task of the stage starts after a
  task completes TERMINAL, STOPPED or CANCELED, or FAILED_CONTINUE converted by
  ``continuePipelineOnFailure``. A task returning ``TaskResult.failed_continue``
  itself lets the stage continue.
* **Recovery** re-issues, for one lost message, exactly the message the normal
  path would have pushed, rebuilt from committed state; it never re-runs a task
  whose result was committed.

Several queue tables in one store
---------------------------------

Processors may use different queue tables (``table_name``) on one store, each
with its own task registry. Then:

* start workflows through ``Orchestrator(queue).start()`` with the queue the
  workflow belongs to, so its ``queue_table`` is recorded before any recovery
  sweep can see it;
* each processor recovers only the workflows whose ``queue_table`` is its own;
  rows written before 1.0.0 have no ``queue_table`` and belong to
  ``queue_messages``;
* processed-message keys of a table other than ``queue_messages`` are
  ``"<table>:<row id>"``; records written before 1.0.0 for such a table under
  the bare row id stop matching after the upgrade, so a message of that table
  in flight across the upgrade can be handled once more.
