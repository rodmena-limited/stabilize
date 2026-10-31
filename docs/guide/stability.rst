Stability and delivery guarantees
=================================

0.34.0 is the release that 1.0.0 promotes unchanged once it has run in
production. What this page lists is what 1.x keeps.

Public API
----------

The public API of stabilize, as of 0.34.0, is:

* every name in ``stabilize.__all__``;
* the message types in ``stabilize.queue.messages`` and their fields;
* ``PoolOptions`` and its fields, ``tenant_setting`` and ``audit_key`` included;
  ``QueueProcessorConfig`` and ``HandlerConfig`` and their fields;
  ``WorkflowCriteria`` and ``StageCriteria`` and their fields;
  ``PostgresEventStore``, ``SqliteEventStore``, ``EventStore`` and
  ``SubscriptionManager``;
* ``TimerTask``, ``WaitTask`` and ``TaskResult.scheduled_time``;
* tenancy mode, the ``stabilize_tenant`` column, ``Workflow.tenant``,
  ``stabilize.current_tenant()`` and the ``tenant`` argument of every store,
  queue and event store;
* the environment variables ``STABILIZE_AUDIT_KEY``,
  ``STABILIZE_RETRY_ON_SHUTDOWN``, ``STABILIZE_STRICT_DELIVERY``,
  ``STABILIZE_TASK_LEASE``, the ``STABILIZE_SQLITE_*`` tuning variables, and
  the ``STABILIZE_RECOVERY_*`` and handler variables ``HandlerConfig`` reads;
* the keys of a ``Queue.list_dlq()`` entry;
* the persisted schema created by ``stabilize mg-up`` (PostgreSQL) and by
  ``SqliteWorkflowStore(create_tables=True)`` (SQLite), including the engine
  context keys enumerated in ``stabilize.models.stage.engine_keys``;
* the event types in ``stabilize.events.EventType``, their payloads and
  ``schema_version`` 2;
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
* every migration within 1.x is additive or widening. Additive means a new
  table, column, index, trigger or function, or a new default. Widening means
  a column accepts what it accepted before and more: a longer ``VARCHAR``, a
  dropped ``NOT NULL``, a ``BIGINT`` for an ``INTEGER``. The one exception in
  0.34.0 is ``processed_messages.processed_at``, which became
  ``TIMESTAMPTZ``; ``mg-up`` checks the server's zone against the writers'
  zone before converting and refuses to guess. ``mg-up`` upgrades a 0.30 to
  0.33 database in place with in-flight work resumed, and hands databases
  built by 0.32.1 and 0.33.x over to migretti (``tests/test_mg_up_takeover.py``).

Deprecated in 0.34.0, removed in 2.0.0:

* ``MultiInstanceBuilder.create_fixed(cancel_remaining=True)`` and
  ``create_from_collection(cancel_remaining=True)``, refused because the
  instances past the threshold are already running when it is reached;
* ``WorkflowRecovery(batch_size=...)``, accepted and ignored because recovery
  pages its candidates itself;
* ``Orchestrator.get_instance()``, refused in tenancy mode;
* ``register_all_handlers()``.

Rolling back to 0.33.x is a code change only. The 0.34.0 migrations stay in
place and 0.33.x reads past the added columns. Drain the queue first, because
0.33.x drops the fields 0.34.0 adds to a message. A schema in tenancy mode
with data in it cannot go back to 0.33.x, which has no tenant; export, recreate
the schema and import instead. Events written by 0.34.0 carry
``schema_version`` 2 and are skipped by a 0.33.x replay.

Floors
------

* **Python** 3.11, 3.12, 3.13 and 3.14 are tested on every release. 3.15 is
  supported by bracketing: nothing in the engine is version-specific, and the
  one dependency without a 3.15 wheel is pyyaml. A free-threaded build with
  the GIL disabled is refused when a ``QueueProcessor`` is built
  (``tests/test_floors.py``).
* **PostgreSQL** 14 is the floor; 14, 16 and 18 are tested. A major is
  dropped in the first minor release after its end of life, so 14 leaves in
  the release after 2026-11-12. PostgreSQL 19 is tested from the first
  release after its general availability.
* **SQLite** 3.35.0 is the floor, checked when a connection is opened
  (``RETURNING``). ``:memory:`` databases are per connection.
* **psycopg** 3.0, **psycopg_pool** 3.2 and **python-ulid** 3.2, whose
  monotonic ids task order within a stage depends on.

Clocks
------

Workflow, stage and task times are milliseconds since the epoch, taken from
the engine host. Queue timing (``deliver_at``, ``locked_until``) and retention
use the database server's clock. Keep the two within the queue's
``lock_duration`` of each other. ``processed_at`` is stored with its zone from
0.34.0, so retention no longer moves with the session zone.

Threads, processes and connection poolers
-----------------------------------------

Stores, queues and event stores are safe to share between threads. A
``QueueProcessor`` runs its handlers on a thread pool and its tasks in a
bulkhead; ``ProcessIsolatedTaskExecutor`` runs a task in a child process and
brings its exception back whole. One SQLite file is one tenant per process: a
second tenant on the same file is refused. Several processes may share a
PostgreSQL database and a queue table; they coordinate through row locks and
advisory locks keyed by tenant.

PgBouncer in session mode is supported. In transaction mode the engine's
tenant setting is transaction-local and survives, but psycopg's server-side
prepared statements do not: use PgBouncer 1.21 or newer with
``max_prepared_statements`` set, or pass
``PoolOptions(connect_kwargs={"prepare_threshold": None})``, and set
``server_reset_query_always = 1``. The engine refuses a connection whose
tenant setting carries another tenant's value and names the value.

Isolation
---------

In tenancy mode every row belongs to one tenant. A store, queue or event store
reads and writes its own tenant's rows only; the fence trigger refuses a write
without a tenant setting or with the wrong one; subscription ids, admission
locks and recovery locks are namespaced by tenant. A ``WorkflowStream`` passes
only its tenant's live events. ``tests/test_tenant_isolation.py`` and
``audit/evaluations/probe_multitenant_rls.py`` exercise it on both backends.

Delivery guarantees
-------------------

These are what the test suite and the ``audit/evaluations`` probes exercise;
they are stated as measured, not as aspirations.

* **Messages are delivered at least once and handled effectively once.** A
  handler marks its message processed in the same transaction as its effects
  and its pushes, and a mark that finds no row fails the transaction; a
  redelivered message whose record exists is acknowledged without being
  handled again. Records older than ``processed_messages_max_age_hours`` are
  removed by the retention sweep.
* **A task's side effects can repeat in these situations**, and only these:
  a worker's lock renewal fails for longer than the queue's ``lock_duration``
  (another worker then takes the message; enable the task lease to exclude
  this); a worker dies after the task's side effect and before its result
  commits; a task returns ``RUNNING`` and is polled again, by design; a task
  result held because the store was unavailable is lost when the process
  exits before the store returns, and the task runs again on redelivery.
* **A store outage is not a workflow failure.** A handler that cannot reach
  the store re-raises, the message is redelivered with a growing delay, and
  it moves to the dead-letter queue only after ``store_retry_budget`` (30
  minutes) of continuous failure. Transient database errors (deadlock,
  serialisation, lock timeout, connection refused) retry a task as before.
* **Task hooks** (``on_timeout``, ``on_cancel``, ``on_cleanup``) and stage
  finalizers run once per committed transition, after the commit. The
  exception is ``on_cancel`` for a task cancelled by ``CancelStage``, which is
  pushed as a message in the cancelling transaction and runs at least once.
* **A failed task ends its stage**: no later task of the stage starts after a
  task completes TERMINAL, STOPPED or CANCELED, or FAILED_CONTINUE converted by
  ``continuePipelineOnFailure``.
* **A deferred choice has one winner.** The losers end SKIPPED with
  ``_pruned``. A group decides on start or on completion as
  ``deferredChoiceClaim`` says. Settlement is serialised under an advisory
  lock, so two members finishing at once produce one outcome.
* **Resets are complete.** A jump, restart or multi-merge re-arm clears the
  stage's claims, engine keys, signals and queued messages and raises the
  task generation; a result from an earlier generation is dropped.
* **Timers are durable.** ``TimerTask`` and ``WaitTask`` record their due
  time in ``scheduled_time``, and a drain returns when nothing is due.
* **Recovery** re-issues, for one lost message, exactly the message the normal
  path would have pushed, rebuilt from committed state; it never re-runs a task
  whose result was committed. It runs per workflow under a lock, so two sweeps
  cannot both push. A workflow with no activity for ``recovery_max_age_hours``
  (24 by default) is reported ``stale`` and left alone. Activity is the latest
  of its rows' changes, start and end times, timers and signals, not its start
  time. ``tests/test_recovery_window.py`` and
  ``audit/evaluations/probe_recovery_examines_all.py`` probe both directions.
* **Signals are delivered once.** A signal is consumed in the transaction
  that saves the task's result and buffered in the transaction that
  suspends the stage. Recovery re-pushes a buffered signal that was never
  consumed.
* **Retention keeps live dedup records**: the processor's retention sweep and
  ``stabilize prune-processed`` delete a ``processed_messages`` record only when
  its message is no longer in its queue table.
* **Event purge keeps subscriptions whole**: ``stabilize purge-events`` and
  ``EventStore.purge_events`` refuse while an enabled durable subscription still
  has undelivered events inside the bound. Pass ``--ignore-subscriptions`` to
  purge anyway.
* **Durable subscriptions deliver every committed event at least once**, in
  commit order on PostgreSQL, with a position that survives a full batch and a
  restart.

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
  ``"<table>:<row id>"``;
* on PostgreSQL, ``stabilize mg-queue-table <name>`` creates a further queue
  table in the schema's shape with its indexes. A table created by hand needs
  the index that serves recovery and ``purge-workflows``::

      CREATE INDEX IF NOT EXISTS idx_<table>_execution_id
          ON <table> ((payload ->> 'execution_id'));
      CREATE INDEX IF NOT EXISTS idx_<table>_dlq_execution_id
          ON <table>_dlq ((payload ->> 'execution_id'));

  SQLite queue tables get these indexes when the queue creates its tables.
