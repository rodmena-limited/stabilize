Persistence
===========

Stabilize supports two backends. Both support full ACID transactions and optimistic locking.

SQLite
------

Best for development, testing, and single-node workloads.

*   Uses WAL mode for concurrency.
*   Zero configuration required.

.. code-block:: python

    store = SqliteWorkflowStore("sqlite:///:memory:")
    # or
    store = SqliteWorkflowStore("sqlite:///./workflows.db")

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
