"""
PostgreSQL event store implementation.

Provides durable, append-only event storage using PostgreSQL.
Uses BIGSERIAL for sequence numbers and JSONB for event data.
"""

from __future__ import annotations

import threading

from stabilize.events.store.interface import EventStore
from stabilize.events.store.postgres.events import PostgresEventsMixin
from stabilize.events.store.postgres.schema import (
    EVENTS_SCHEMA,
    SNAPSHOTS_SCHEMA,
    SUBSCRIPTIONS_SCHEMA,
)
from stabilize.events.store.postgres.snapshots import PostgresSnapshotsMixin
from stabilize.events.store.postgres.subscriptions import PostgresSubscriptionsMixin
from stabilize.persistence.connection import get_connection_manager


class PostgresEventStore(
    PostgresEventsMixin,
    PostgresSnapshotsMixin,
    PostgresSubscriptionsMixin,
    EventStore,
):
    """
    PostgreSQL implementation of event store.

    Uses connection pooling via ConnectionManager.
    Events are stored with BIGSERIAL sequence numbers for
    global ordering and JSONB for efficient data storage.

    Thread-safety:
    - Uses connection pool from ConnectionManager
    - Each operation gets its own connection
    - Sequence assignment is atomic via BIGSERIAL
    """

    def __init__(
        self,
        connection_string: str,
        create_tables: bool = True,
    ) -> None:
        """
        Initialize PostgreSQL event store.

        Args:
            connection_string: PostgreSQL connection string
            create_tables: Whether to create tables if they don't exist.
        """
        self._connection_string = connection_string
        self._manager = get_connection_manager()
        self._pool = self._manager.get_postgres_pool(connection_string)
        self._lock = threading.Lock()

        if create_tables:
            self._create_tables()

    def _create_tables(self) -> None:
        """Create event tables if they don't exist."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(EVENTS_SCHEMA)
                cur.execute(SNAPSHOTS_SCHEMA)
                cur.execute(SUBSCRIPTIONS_SCHEMA)
            conn.commit()

    def close(self) -> None:
        """Close the connection pool."""
        self._manager.close_postgres_pool(self._connection_string)
