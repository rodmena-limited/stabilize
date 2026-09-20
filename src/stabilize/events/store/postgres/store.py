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
    REQUIRED_COLUMNS,
    REQUIRED_TABLES,
    SNAPSHOTS_SCHEMA,
    SUBSCRIPTIONS_CURSOR_MIGRATION,
    SUBSCRIPTIONS_SCHEMA,
    setup_ddl,
)
from stabilize.events.store.postgres.snapshots import PostgresSnapshotsMixin
from stabilize.events.store.postgres.subscriptions import PostgresSubscriptionsMixin
from stabilize.persistence.connection import get_connection_manager

logger = logging.getLogger(__name__)


class EventStoreSchemaError(RuntimeError):
    """Raised when the event store schema is absent or out of date.

    The store does not create its own tables: a library that issues DDL from a
    constructor forces its consumers to grant a standing CREATE privilege, and
    that grant then consents to whatever DDL a later release decides to run.
    """


def _scalar(row: object) -> object:
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]  # type: ignore[index]


def _pair(row: object) -> tuple[object, object]:
    if isinstance(row, dict):
        return row["table_name"], row["column_name"]
    return row[0], row[1]  # type: ignore[index]


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
        create_tables: bool = False,
        schema: str | None = None,
    ) -> None:
        """
        Initialize PostgreSQL event store.

        Args:
            connection_string: PostgreSQL connection string
            create_tables: Issue DDL from this constructor. Off by default: a
                deployment that separates schema management from runtime gives
                its application role DML only, and a library that creates its
                own tables forces a standing CREATE grant that then consents to
                every later release's DDL. Apply ``setup_ddl()`` as the
                migration operator instead.
            schema: Schema holding the event tables, used to qualify the DDL
                named in a refusal.
        """
        self._connection_string = connection_string
        self._manager = get_connection_manager()
        self._pool = self._manager.get_postgres_pool(connection_string)
        self._lock = threading.Lock()
        self._schema = schema

        if create_tables:
            self._create_tables()
        else:
            self._verify_schema()

    def _verify_schema(self) -> None:
        """Refuse at construction when the schema is absent or out of date."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name = ANY(%s)",
                    (list(REQUIRED_TABLES),),
                )
                present_tables = {_scalar(row) for row in cur.fetchall()}

                cur.execute(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_name = ANY(%s)",
                    (list(REQUIRED_TABLES),),
                )
                present_columns = {_pair(row) for row in cur.fetchall()}

        missing_tables = [name for name in REQUIRED_TABLES if name not in present_tables]
        missing_columns = [
            f"{table}.{column}"
            for table, column in REQUIRED_COLUMNS
            if table in present_tables and (table, column) not in present_columns
        ]

        if missing_tables or missing_columns:
            absent = ", ".join(missing_tables + missing_columns)
            raise EventStoreSchemaError(
                f"The event store schema is incomplete: {absent} "
                f"{'is' if len(missing_tables) + len(missing_columns) == 1 else 'are'} missing. "
                "This store does not create its own tables. Apply the following as the "
                "migration operator, then construct it again:\n\n"
                f"{setup_ddl(self._schema)}"
            )

        has_column = ("events", "commit_xid") in present_columns
        self._commit_xid_available = has_column and self._server_supports_xid8()

    def _server_supports_xid8(self) -> bool:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW server_version_num")
                row = cur.fetchone()
        if row is None:
            return False
        raw = row["server_version_num"] if isinstance(row, dict) else row[0]
        if int(raw) >= MIN_COMMIT_XID_VERSION:
            return True
        logger.warning(
            "PostgreSQL %s is older than %d: durable subscriptions fall back to "
            "sequence-ordered delivery, which can permanently skip an event whose "
            "transaction commits after a later one",
            raw,
            MIN_COMMIT_XID_VERSION,
        )
        return False

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
