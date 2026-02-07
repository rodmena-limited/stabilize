"""
SQLite event store implementation.

Provides durable, append-only event storage using SQLite.
Events are stored with auto-incrementing sequence numbers
for global ordering.
"""

from __future__ import annotations

import sqlite3
import threading

from stabilize.events.store.interface import EventStore
from stabilize.events.store.sqlite.events import SqliteEventStoreMixin
from stabilize.events.store.sqlite.schema import (
    EVENTS_SCHEMA,
    SNAPSHOTS_SCHEMA,
    SUBSCRIPTIONS_SCHEMA,
)
from stabilize.events.store.sqlite.snapshots import SqliteSnapshotsMixin
from stabilize.events.store.sqlite.subscriptions import SqliteSubscriptionsMixin
from stabilize.persistence.connection import get_connection_manager


class SqliteEventStore(
    SqliteEventStoreMixin,
    SqliteSnapshotsMixin,
    SqliteSubscriptionsMixin,
    EventStore,
):
    """
    SQLite implementation of event store.

    Uses the shared ConnectionManager for thread-local connections.
    Events are stored in an append-only table with auto-incrementing
    sequence numbers.

    Thread-safety:
    - Uses thread-local connections from ConnectionManager
    - Sequence assignment is atomic via SQLite's AUTOINCREMENT
    """

    def __init__(
        self,
        connection_string: str,
        create_tables: bool = True,
    ) -> None:
        """
        Initialize SQLite event store.

        Args:
            connection_string: SQLite connection string (e.g., "sqlite:///./app.db")
            create_tables: Whether to create tables if they don't exist.
        """
        self._connection_string = connection_string
        self._lock = threading.Lock()

        if create_tables:
            self._create_tables()

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local connection."""
        return get_connection_manager().get_sqlite_connection(self._connection_string)

    def _create_tables(self) -> None:
        """Create event tables if they don't exist."""
        conn = self._get_connection()
        conn.executescript(EVENTS_SCHEMA)
        conn.executescript(SNAPSHOTS_SCHEMA)
        conn.executescript(SUBSCRIPTIONS_SCHEMA)
        conn.commit()
