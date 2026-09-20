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
        """Refuse at construction when the schema is absent or out of date.

        Resolution, not existence. An information_schema lookup without a schema
        filter passes when the table exists ANYWHERE this role can see, while
        the connection's search_path resolves somewhere else entirely -- so the
        check would go green on the day it stopped being true. to_regclass()
        answers what the name actually binds to on THIS connection, which is the
        artefact the engine will read and write.
        """
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                resolved: dict[str, str] = {}
                namespaces: dict[str, str] = {}
                misplaced: list[str] = []
                for table in REQUIRED_TABLES:
                    # Ask the catalog which NAMESPACE the oid belongs to rather
                    # than reading to_regclass()::text. That rendering omits the
                    # schema exactly when the schema is on the search_path --
                    # the healthy case -- so a comparison against a qualified
                    # name fails when everything is correct, and relaxing it to
                    # the unqualified form then accepts a shadow copy in another
                    # schema. The rendering depends on the setting under test;
                    # the namespace does not.
                    cur.execute(
                        "SELECT to_regclass(%s)::text AS rel, "
                        "  (SELECT n.nspname FROM pg_class c "
                        "     JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "    WHERE c.oid = to_regclass(%s)) AS nsp",
                        (table, table),
                    )
                    row = cur.fetchone()
                    if not row:
                        continue
                    rel = row["rel"] if isinstance(row, dict) else row[0]
                    nsp = row["nsp"] if isinstance(row, dict) else row[1]
                    if not rel:
                        continue
                    if self._schema and nsp != self._schema:
                        misplaced.append(f"{table} resolves to {nsp}.{table}, not {self._schema}")
                        continue
                    resolved[table] = rel
                    if nsp:
                        namespaces[table] = str(nsp)
                present_tables = set(resolved)

                present_columns = set()
                if resolved:
                    cur.execute(
                        "SELECT c.relname AS table_name, a.attname AS column_name "
                        "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                        "WHERE a.attrelid = ANY(%s::regclass[]) AND a.attnum > 0 "
                        "AND NOT a.attisdropped",
                        (list(resolved.values()),),
                    )
                    present_columns = {_pair(row) for row in cur.fetchall()}

        missing_tables = [name for name in REQUIRED_TABLES if name not in present_tables]
        missing_columns = [
            f"{table}.{column}"
            for table, column in REQUIRED_COLUMNS
            if table in present_tables and (table, column) not in present_columns
        ]

        # Without a configured schema= there is nothing to compare each table
        # against -- but the tables must still all resolve to ONE namespace.
        # A shadow copy of a single table ahead of the real one on the
        # search_path splits them, and that is the case this check exists for.
        # Making the protection depend on schema= would give it only to the
        # configuration we advise against: a DSN options segment beats schema=,
        # so the callers following our own advice set no schema at all.
        distinct = sorted(set(namespaces.values()))
        if not self._schema and len(distinct) > 1:
            split = ", ".join(f"{t} in {n}" for t, n in sorted(namespaces.items()))
            raise EventStoreSchemaError(
                "The event store tables resolve to more than one schema on this "
                f"connection ({split}). A shadow copy ahead of the real table on "
                "the search_path splits them, so the engine would read and write "
                "a mixture. Set schema= or fix the search_path."
            )

        self._resolved_schema = distinct[0] if len(distinct) == 1 else None

        if misplaced:
            raise EventStoreSchemaError(
                "The event store schema resolves to the wrong namespace: "
                + "; ".join(misplaced)
                + ". The connection's search_path does not reach the schema this "
                "store was configured for, so it would read and write tables the "
                "operator did not intend."
            )

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

    def resolved_schema(self) -> str | None:
        """The namespace the event tables actually resolved to at construction.

        THIS IS THE ONLY COVER FOR ONE CASE, not a convenience beside the
        checks. Without a configured ``schema=`` the store can assert that its
        tables resolve CONSISTENTLY -- a shadow of one table splits the set and
        is refused -- but a complete shadow of ALL of them resolves consistently,
        agrees with itself, and passes while the engine reads and writes the
        wrong tables entirely.

        No engine-side check can close that: with no configured expectation
        there is nothing to compare against, and internal consistency is the
        most an unconfigured check can honestly assert. A caller who sets no
        schema and never reads this value has no protection against the whole
        set being shadowed. Read it and compare it against what you expect, or
        set ``schema=`` and let the store refuse.
        """
        return getattr(self, "_resolved_schema", None)

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
