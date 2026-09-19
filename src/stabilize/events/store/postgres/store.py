"""
PostgreSQL event store implementation.

Provides durable, append-only event storage using PostgreSQL.
Uses BIGSERIAL for sequence numbers and JSONB for event data.
"""

from __future__ import annotations

import logging
import threading

from stabilize.events.store.interface import EventStore
from stabilize.events.store.postgres.events import PostgresEventsMixin
from stabilize.events.store.postgres.schema import (
    EVENTS_COMMIT_XID_MIGRATION,
    EVENTS_SCHEMA,
    MIN_COMMIT_XID_VERSION,
    SNAPSHOTS_SCHEMA,
    SUBSCRIPTIONS_CURSOR_MIGRATION,
    SUBSCRIPTIONS_SCHEMA,
)
from stabilize.events.store.postgres.snapshots import PostgresSnapshotsMixin
from stabilize.events.store.postgres.subscriptions import PostgresSubscriptionsMixin
from stabilize.persistence.connection import get_connection_manager

logger = logging.getLogger(__name__)


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
                for statement in SUBSCRIPTIONS_CURSOR_MIGRATION:
                    cur.execute(statement)
            conn.commit()
        self._apply_commit_xid_migration()

    def _apply_commit_xid_migration(self) -> None:
        """Add the commit-ordering column where the server supports it.

        Skipped below PostgreSQL 13, which has no xid8. Subscriptions then fall
        back to the sequence cursor, which can skip an event whose transaction
        commits out of sequence order.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW server_version_num")
                row = cur.fetchone()
                if row is None:
                    self._commit_xid_available = False
                    return
                raw = row["server_version_num"] if isinstance(row, dict) else row[0]
                version = int(raw)
                if version < MIN_COMMIT_XID_VERSION:
                    self._commit_xid_available = False
                    logger.warning(
                        "PostgreSQL %d is older than %d: durable subscriptions fall back to "
                        "sequence-ordered delivery, which can permanently skip an event whose "
                        "transaction commits after a later one",
                        version,
                        MIN_COMMIT_XID_VERSION,
                    )
                    return
                for statement in EVENTS_COMMIT_XID_MIGRATION:
                    cur.execute(statement)
            conn.commit()
        self._commit_xid_available = True

    def supports_commit_cursor(self) -> bool:
        """Whether commit-ordered subscription delivery is available."""
        return bool(getattr(self, "_commit_xid_available", False))

    def close(self) -> None:
        """Close the connection pool."""
        self._manager.close_postgres_pool(self._connection_string)
