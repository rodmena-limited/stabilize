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

.. code-block:: python

    store = PostgresWorkflowStore("postgres://user:pass@localhost:5432/stabilize")
