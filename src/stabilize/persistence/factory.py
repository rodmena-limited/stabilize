"""
Factory functions for creating database backends.

Automatically selects PostgreSQL or SQLite based on connection string.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue.queue import Queue


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
) -> WorkflowStore:
    """
    Create an execution repository based on the connection string.

    Automatically detects whether to use PostgreSQL or SQLite based
    on the connection string prefix.

    Args:
        connection_string: Database connection URL
        create_tables: Whether to create tables if they don't exist
                      (SQLite only - PostgreSQL uses migrations)

    Returns:
        WorkflowStore: PostgreSQL or SQLite repository instance

    Note:
        PostgreSQL requires running migrations first:
            stabilize migrate --database <connection_string>

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

        return SqliteWorkflowStore(connection_string, create_tables)
    else:
        from stabilize.persistence.postgres import PostgresWorkflowStore

        return PostgresWorkflowStore(connection_string)


def create_queue(
    connection_string: str,
    table_name: str = "queue_messages",
    lock_duration: timedelta | None = None,
    max_attempts: int = 10,
) -> Queue:
    """
    Create a message queue based on the connection string.

    Automatically detects whether to use PostgreSQL or SQLite based
    on the connection string prefix.

    Args:
        connection_string: Database connection URL
        table_name: Name of the queue table
        lock_duration: How long to lock messages during processing
        max_attempts: Maximum retry attempts before dropping message

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

    if backend == "sqlite":
        from stabilize.queue.sqlite_queue import SqliteQueue

        return SqliteQueue(
            connection_string,
            table_name=table_name,
            lock_duration=lock_duration,
            max_attempts=max_attempts,
        )
    else:
        from stabilize.queue.queue import PostgresQueue

        return PostgresQueue(
            connection_string,
            table_name=table_name,
            lock_duration=lock_duration,
            max_attempts=max_attempts,
        )
