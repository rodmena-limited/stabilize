Persistence
===========

Stabilize supports two backends. Both support full ACID transactions and optimistic locking.

SQLite
------

Best for development, testing, and single-node workloads.

*   Zero configuration required — the schema is created automatically.
*   Uses the ``DELETE`` journal by default for simple, predictable locking.

.. code-block:: python

    store = SqliteWorkflowStore("sqlite:///:memory:")
    # or
    store = SqliteWorkflowStore("sqlite:///./workflows.db")

Journal mode (WAL, opt-in)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default journal mode is ``DELETE``. For higher read/write concurrency on a
single node you can opt into **WAL** (Write-Ahead Logging) without any code
change:

.. code-block:: bash

    export STABILIZE_SQLITE_JOURNAL_MODE=WAL

Or programmatically:

.. code-block:: python

    from stabilize.persistence.sqlite_config import SqliteConfig

    config = SqliteConfig(journal_mode="WAL")  # default is "DELETE"

The value is validated against an allow-list (``DELETE``, ``TRUNCATE``,
``PERSIST``, ``MEMORY``, ``WAL``, ``OFF``); an unknown value safely falls back
to ``DELETE``. When WAL is active, ``wal_autocheckpoint`` is applied per the
optimization tier.

Schema migrations
~~~~~~~~~~~~~~~~~~

The SQLite schema is versioned. ``create_tables()`` installs the baseline schema
(version 1) and then runs a small forward-only migration runner that records
applied versions in a ``schema_migrations`` table. This means:

*   New databases are stamped at the baseline version automatically.
*   **Existing, pre-versioning databases upgrade in place** — they are stamped at
    the baseline without re-running the baseline DDL, then any newer migrations
    are applied in order.
*   Re-running the migrator is idempotent.

Future schema changes are added as ordered, additive entries to
``stabilize.persistence.sqlite.migrations.MIGRATIONS``.

PostgreSQL
----------

Required for "Airport Grade" production deployments.

*   Uses ``psycopg`` (v3) connection pooling.
*   Supports massive concurrency via ``SKIP LOCKED`` (in queue implementation) and efficient indexing.

.. code-block:: bash

    # Migrations
    export MG_DATABASE_URL="postgres://user:pass@localhost:5432/stabilize"
    stabilize mg-up

``mg-up`` runs migretti (``mg verify``, then ``mg apply``) on the migration files
shipped in the package. migretti records them in ``_migrations`` in the target
schema, or in a history schema of their own (below). On a database set up by
stabilize 0.33.1 or earlier, the first ``mg-up`` records the existing history in
migretti without running the SQL again and leaves the old ``stabilize_migrations``
table in place.

A schema that has stabilize's tables and no migration history at all is adopted.
``mg-up`` compares the catalog (tables, column types, indexes, sequence types) with
what each migration's SQL creates, records the migrations up to the level the
schema matches without running them, and applies the rest. ``mg-status`` marks
those migrations ``untracked`` until then.

``mg-up`` stops with exit 1 before changing anything when an applied migration's
file has changed, when a history row matches no shipped version of its file, or
when an untracked schema matches no migration level. The refusal names the
mismatching tables and columns. Concurrent runs against one database wait for each
other, up to ``--lock-timeout`` seconds (default 900).

By default tables are created in the connection's default schema (usually
``public``). To keep them in a dedicated schema, add ``?schema=`` to the URL
(or set ``MG_SCHEMA``, or a ``schema:`` key under ``database:`` in the file
named by ``--config``); ``mg-up`` creates the schema if missing and applies all
migrations into it. Point the runtime store at the same schema with a
``search_path`` option in the DSN:

.. code-block:: bash

    export MG_DATABASE_URL="postgres://user:pass@localhost:5432/stabilize?schema=stabilize"
    stabilize mg-up

.. code-block:: python

    store = PostgresWorkflowStore(
        "postgres://user:pass@localhost:5432/stabilize?options=-csearch_path%3Dstabilize"
    )

Do not name the schema after the connecting role. PostgreSQL's default
``search_path`` is ``"$user", public``, so with a role called ``stabilize`` a
schema called ``stabilize`` is searched before ``public`` for every unqualified
name, and another project's migretti then reads stabilize's ``_migrations`` as
its own. ``mg-up`` refuses that combination (0.34.0). The examples above assume
the role is not called ``stabilize``.

Without ``?schema=``, both the migrator and the runtime store use the
connection's default schema and the DSN needs no options:

.. code-block:: python

    store = PostgresWorkflowStore("postgres://user:pass@localhost:5432/stabilize")

The CLI reads a URL as libpq does. When a URL names the host, port, user or
database both in the URL and as a query parameter (``?host=``, ``?dbname=``), the
query parameter wins, so ``mg-up`` migrates the database the engine connects to.
A URL without a user or port leaves them to libpq's defaults (``PGUSER``,
``PGPORT``, the operating system user), as the engine does.
``schema`` and ``history_schema`` are stabilize's own parameters and are not passed
to libpq. ``MG_SCHEMA`` and ``MG_HISTORY_SCHEMA`` apply only when the URL names
neither.

Sharing a schema with your own migretti history
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

migretti keeps one ``_migrations`` table per schema. If stabilize's tables live in
a schema whose ``_migrations`` belongs to your project (usually ``public``), keep
stabilize's history in a schema of its own:

.. code-block:: bash

    stabilize mg-up --db-url "$DATABASE_URL" --history-schema stabilize_history

The tables stay in ``public``. Only ``_migrations`` and ``_migrations_log`` go to
``stabilize_history``. The URL parameter ``history_schema=`` and the environment
variable ``MG_HISTORY_SCHEMA`` do the same, and ``mg-status`` takes the same flag.
Later runs find the history schema without the flag, whatever its name.

If your own migrations alter stabilize's tables, run ``mg-up`` before them on a
fresh database. An install that predates 0.33.3, with ``stabilize_migrations``
beside your project's history, is taken over into ``stabilize_history`` on the first
``mg-up`` without moving any table or running any migration SQL again.

``mg-up`` refuses, before creating anything, when the history schema is the target
schema, when stabilize's history is already in the target schema, and when the
history schema holds your project's history or the history of stabilize's tables
in a different schema. Two stabilize schemas in one database each keep their own
history.

.. warning::

   Scope ``search_path`` to the connections Stabilize owns — the workflow
   store, the queue, and the event store. Do **not** apply it to a database URL
   that non-Stabilize application code also uses.

   In a hybrid deployment (application tables in ``public``, Stabilize tables in
   ``stabilize``) a globally applied ``search_path`` silently re-resolves every
   unqualified application query whose table name Stabilize also uses. The
   reported case is ``snapshots``: an application register table of that name
   and the event store's own ``snapshots`` table both exist, so a bare
   ``SELECT ... FROM snapshots`` in the application starts reading the event
   store instead. Nothing errors — the query simply returns the wrong rows.

   For the same reason, administrative or housekeeping queries that read
   Stabilize tables over an unscoped pool must schema-qualify their table names
   (``stabilize.pipeline_executions``, not ``pipeline_executions``).

   A Stabilize-only database has no such collision, and can set the option on
   the single shared DSN.

Pool options and idle cost
~~~~~~~~~~~~~~~~~~~~~~~~~~

``PostgresWorkflowStore`` and ``PostgresQueue`` both accept ``options=PoolOptions(...)``.
Pools are keyed by connection string and options, so pass the **same** ``PoolOptions``
instance to both to share one pool:

.. code-block:: python

    from stabilize.persistence.pool_options import PoolOptions

    options = PoolOptions(min_size=1, max_size=4)
    store = PostgresWorkflowStore(dsn, options=options)
    queue = PostgresQueue(dsn, options=options)

Without options each gets the default pool (``min_size=5``, ``max_size=15``).

``check``, ``reset`` and ``max_idle`` are passed to psycopg_pool's
``ConnectionPool``. ``check`` runs when a connection is handed out and ``reset``
when it is returned. ``max_idle`` is how many seconds an idle connection above
``min_size`` is kept (psycopg_pool's default is 600). They need psycopg-pool 3.2.0
or later.

``on_borrow`` runs each time the engine borrows a connection, before any engine
statement, for reads and transactions alike. It must leave the connection outside
a transaction, by committing or by using autocommit. A borrow it leaves inside a
transaction fails with ``stabilize.persistence.borrow_hook.BorrowHookError``, and
the connection goes back to the pool. An exception raised in ``on_borrow`` reaches
the caller.

Several tenants on one database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

From 0.34.0 the engine keeps tenants apart itself. Put the schema in tenancy mode (see
`Tenancy mode`_ below), then build one set of parts per tenant and one processor per
tenant process:

.. code-block:: python

    options = PoolOptions(tenant_setting="app.current_tenant")
    store = PostgresWorkflowStore(dsn, tenant="acme", options=options)
    queue = PostgresQueue(dsn, tenant="acme", options=options)
    events = PostgresEventStore(dsn, tenant="acme", options=options)
    processor = QueueProcessor(queue, store=store, ...)
    processor.start()

Every row the engine writes carries ``stabilize_tenant = 'acme'``, every statement it
runs filters on that value, and the fence refuses a write from any connection that does
not carry the tenant. The parts for different tenants may share one pool. On each
transaction the engine sets ``stabilize.tenant``. With ``tenant_setting`` it sets your own
setting too, and a policy of yours written against ``current_setting('app.current_tenant',
true)`` keeps working. ``tests/test_tenant_isolation.py`` runs two tenants through every
public operation of the store, queue and event store on both backends and checks that
neither sees the other's rows.

The 0.33 recipe, a session-level ``on_borrow`` hook that sets a host setting from a
``ContextVar`` before ``QueueProcessor.start()``, still works for a single-tenant install
that keeps its own ``tenant_id`` column and policies. It no longer isolates tenants on
its own in 0.34: use tenancy mode for that.

An idle ``QueueProcessor`` polls once per ``poll_frequency_ms`` and every poll is one
transaction. Measured on PostgreSQL 16 for one idle processor:

=====================  ===================  ==================
``poll_frequency_ms``  transactions/second  transactions/day
=====================  ===================  ==================
50 (default)           19.6                 about 1.7 million
1000                   1.2                  about 0.1 million
=====================  ===================  ==================

Raise ``poll_frequency_ms`` where pickup latency allows; the same cost applies per
processor instance.

Upgrading to 0.34
-----------------

Stop every engine process first. A 0.33 process kept running across ``mg-up`` fails
its next read, because the shape of the rows it prepared changed under it. Start the
processes again only after ``mg-up`` prints OK.

**A single-tenant install coming from 0.30 to 0.33** (Provenance, Trace):

.. code-block:: bash

    stabilize mg-up --db-url "$DSN"
    stabilize mg-check-grants --role "$RUNTIME_ROLE" --db-url "$DSN"

``mg-up`` adds the tenant column and the mode record and keeps the schema in single-tenant
mode. Grant the runtime role ``SELECT ON stabilize_tenancy``. Nothing in the workflow
code changes. A row-level security setup of your own keeps working, and
``PoolOptions(tenant_setting=...)`` is available when you want the engine to set your
setting on each transaction.

**A fresh install with tenants** (Casebound):

.. code-block:: bash

    stabilize mg-up --db-url "$DSN"
    stabilize mg-tenancy enable --db-url "$DSN"
    stabilize mg-check-grants --role "$RUNTIME_ROLE" --db-url "$DSN"

Then your own migrations, with the policy from `The fence`_ if you keep policies, and
one process per tenant built with ``tenant="..."``. ``mg-tenancy enable`` works only
while every engine table is empty. Run it before the first workflow.

**A host that owns its DDL** (ci-conductor). Take the migration files from the package
with ``stabilize.cli.migrations.get_migration_files()``, or ``PostgresEventStore.setup_ddl()``
for the event tables alone. Apply them with your own tool and run ``mg-check-grants``.
When a table differs from what the engine expects, the first-use schema check names
the DDL to run.

Tenancy mode
------------

Every engine table carries a ``stabilize_tenant`` column. A schema starts in
single-tenant mode. There the column is ``''`` on every row and the engine filters on
it without anyone noticing. In tenancy mode every store, queue and event store is
built for one tenant. It stamps that tenant on each row it writes and reads only rows
that carry it.

.. code-block:: bash

    stabilize mg-up --db-url "$DSN"
    stabilize mg-tenancy enable --db-url "$DSN"
    stabilize mg-tenancy status --db-url "$DSN"

``mg-tenancy enable`` and ``disable`` work only on a schema with no rows in any
engine table and with every shipped migration applied. Custom queue tables are found
by shape or named with ``--queue-table``. The command takes the ``mg-up`` lock and
changes nothing when a precondition fails. Stop every engine process before running
it and start them again afterwards. Converting a schema that holds data means
exporting it, recreating the schema in tenancy mode and importing. For a SQLite file,
pass a ``sqlite:///path`` URL.

.. code-block:: python

    store = PostgresWorkflowStore(dsn, tenant="acme")
    queue = PostgresQueue(dsn, tenant="acme")
    events = PostgresEventStore(dsn, tenant="acme")

A tenant is 1 to 64 characters from ``A-Z a-z 0-9 . : -``. In tenancy mode a part
built without a tenant is refused at its first statement with ``TenancyModeError``.
In single-tenant mode a part built with a tenant gets the same refusal. Parts carrying
different tenants cannot be combined in a ``QueueProcessor``, an ``Orchestrator`` or
``store.transaction(queue)``. A write that collides with a row another tenant owns
raises ``TenantMismatchError``.

The runtime role needs ``SELECT`` on ``stabilize_tenancy``, and ``mg-check-grants``
reports whether it has it. Never put a row-level security policy on that table. The
monitor takes ``--tenant NAME`` or ``--all-tenants``. In tenancy mode one of them is
required.

The fence
~~~~~~~~~

In tenancy mode the engine sets the PostgreSQL setting ``stabilize.tenant`` on every
transaction it runs, with ``set_config(..., true)``, and ``mg-tenancy enable`` installs
a trigger named ``stabilize_tenant_fence`` on every engine table. The trigger refuses a
write when the setting is empty (SQLSTATE ``ST001``), when it differs from the row's
tenant (``ST002``), and when it is ``'*'`` from a role that is not a member of the table
owner (``ST003``). ``enable`` also adds the constraint ``stabilize_tenant_nonempty`` and
drops the ``''`` default. After that a 0.33 process or a hand-written insert cannot add
a row without a tenant. The engine refuses a connection that already carries
``stabilize.tenant`` from a hook, ``ALTER ROLE`` or ``ALTER DATABASE``, and refuses a
connection in autocommit mode, because the setting is transaction-local.

Your own writes to engine tables, for example a data fix, run as the table owner with
``SET LOCAL stabilize.tenant = '*'`` or as the tenant with ``SET LOCAL stabilize.tenant
= 'acme'``. A policy of your own can reference the same setting:

.. code-block:: sql

    CREATE POLICY tenant_rows ON stage_executions AS RESTRICTIVE
        USING (stabilize_tenant = current_setting('stabilize.tenant', true)
               OR (current_setting('stabilize.tenant', true) = '*'
                   AND stabilize_is_table_owner('stage_executions'::regclass)));

``PoolOptions(tenant_setting="app.current_tenant")`` makes the engine set your own
setting to the tenant on each transaction as well, for policies written against it.
``enable`` refuses a schema with ``FORCE ROW LEVEL SECURITY`` on an engine table and a
database or role that already sets ``stabilize.tenant``.

A custom queue table needs a ``_dlq`` sibling with the shape of ``queue_messages_dlq``,
because the engine moves poison messages there. ``stabilize mg-queue-table NAME`` creates
both in the target schema. In tenancy mode it adds the fence to them. A second run
changes nothing.

On its first statement each store, queue and event store checks the shape of the
tables it uses and refuses with ``EngineSchemaError`` when a column, the fence or the
tenant default is wrong. The message is the DDL to run as the owner. ``mg-up`` keeps the
shape right. The check catches a schema changed by hand and a process started against
the wrong database.

A processor whose statements the fence refuses records the error as its ``fault``,
stops polling and leaves the message it was handling for a process built for the new
mode. It does not start again. Watch ``processor.fault`` and restart the service after
``mg-tenancy``.

On SQLite the fence is a set of triggers on every engine table, installed by
``mg-tenancy enable`` and by the engine when it creates a table in a tenancy-mode file.
They compare each row with ``stabilize_tenant()``, a function stabilize registers on
every connection it opens. A process binds one tenant per file. Two tenants on one
SQLite file therefore need two processes. The ``sqlite3`` shell has no
``stabilize_tenant()`` and cannot write to a tenancy-mode file. It can still ``DROP
TRIGGER``.

