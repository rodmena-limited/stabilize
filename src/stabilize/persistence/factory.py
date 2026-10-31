"""
Factory functions for creating database backends.

Automatically selects PostgreSQL or SQLite based on connection string.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from stabilize.persistence.pool_options import PoolOptions

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue


def detect_backend(connection_string: str) -> str:
    """
    Detect the database backend type from a connection string.

    Args:
        connection_string: Database connection URL

    Returns:
        "sqlite" or "postgresql"

    Examples:
        >>> detect_backend("sqlite:///./db.sqlite")
        'sqlite'
        >>> detect_backend("sqlite:///:memory:")
        'sqlite'
        >>> detect_backend("postgresql+psycopg://user:pass@localhost/db")
        'postgresql'
    """
    if connection_string.startswith("sqlite"):
        return "sqlite"
    return "postgresql"


def create_repository(
    connection_string: str,
    create_tables: bool = False,
    tenant: str = "",
    *,
    schema: str | None = None,
    options: PoolOptions | None = None,
) -> WorkflowStore:
    """
    Create an execution repository based on the connection string.

    Automatically detects whether to use PostgreSQL or SQLite based
    on the connection string prefix.

    Args:
        connection_string: Database connection URL
        create_tables: Whether to create tables if they don't exist
                      (SQLite only - PostgreSQL uses migrations)
        tenant: The tenant this store reads and writes for ('' in single-tenant mode)
        schema: PostgreSQL schema holding the engine tables (default search_path)
        options: PostgreSQL pool options (sizes, hooks, tenant_setting, audit_key)

    Returns:
        WorkflowStore: PostgreSQL or SQLite repository instance

    Note:
        PostgreSQL requires running migrations first:
            stabilize mg-up --db-url <connection_string>

    Examples:
        # PostgreSQL (requires migrations)
        repo = create_repository(
            "postgresql+psycopg://user:pass@localhost/stabilize"
        )

        # SQLite file-based
        repo = create_repository(
            "sqlite:///./stabilize.db",
            create_tables=True
        )

        # SQLite in-memory (for testing)
        repo = create_repository(
            "sqlite:///:memory:",
            create_tables=True
        )
    """
    backend = detect_backend(connection_string)

    if backend == "sqlite":
        from stabilize.persistence.sqlite import SqliteWorkflowStore

        return SqliteWorkflowStore(connection_string, create_tables, tenant=tenant)
    else:
        from stabilize.persistence.postgres import PostgresWorkflowStore

        return PostgresWorkflowStore(connection_string, schema=schema, options=options, tenant=tenant)


def create_queue(
    connection_string: str,
    table_name: str = "queue_messages",
    lock_duration: timedelta | None = None,
    max_attempts: int = 10,
    tenant: str = "",
    *,
    schema: str | None = None,
    options: PoolOptions | None = None,
    store_retry_budget: timedelta | None = None,
) -> Queue:
    """
    Create a message queue based on the connection string.

    Automatically detects whether to use PostgreSQL or SQLite based
    on the connection string prefix.

    Args:
        connection_string: Database connection URL
        table_name: Name of the queue table
        lock_duration: How long to lock messages during processing
        max_attempts: Deliveries before a message moves to the dead-letter queue
        tenant: The tenant this queue serves ('' in single-tenant mode)
        schema: PostgreSQL schema holding the queue tables (default search_path)
        options: PostgreSQL pool options
        store_retry_budget: How long a message redelivered for a store outage stays
            deliverable past max_attempts (default 30 minutes)

    Returns:
        Queue: PostgreSQL or SQLite queue instance

    Examples:
        # PostgreSQL (uses FOR UPDATE SKIP LOCKED)
        queue = create_queue("postgresql+psycopg://user:pass@localhost/db")

        # SQLite (uses optimistic locking)
        queue = create_queue("sqlite:///./stabilize.db")
    """
    backend = detect_backend(connection_string)
    lock_duration = lock_duration or timedelta(minutes=5)
    budget: dict[str, timedelta] = {} if store_retry_budget is None else {"store_retry_budget": store_retry_budget}

    if backend == "sqlite":
        from stabilize.queue.sqlite import SqliteQueue

        return SqliteQueue(
            connection_string,
            table_name=table_name,
            lock_duration=lock_duration,
            max_attempts=max_attempts,
            tenant=tenant,
            **budget,
        )
    else:
        from stabilize.queue import PostgresQueue

        return PostgresQueue(
            connection_string,
            table_name=table_name,
            lock_duration=lock_duration,
            max_attempts=max_attempts,
            tenant=tenant,
            schema=schema,
            options=options,
            **budget,
        )
