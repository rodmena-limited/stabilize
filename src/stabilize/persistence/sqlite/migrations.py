"""Lightweight forward-only schema migrations for SQLite.

SQLite installs the baseline schema (see :data:`stabilize.persistence.sqlite.schema.SCHEMA`)
as version 1. Future schema changes are appended to :data:`MIGRATIONS` as ordered,
additive steps. A ``schema_migrations`` table records which versions have been applied
so existing databases upgrade *in place* without re-running the baseline DDL.

Design goals:
- **No regression:** a freshly created database is stamped at the baseline version
  and behaves exactly as before; only an extra bookkeeping table is added.
- **Safe upgrades:** an existing, un-versioned customer database is detected and
  stamped at the baseline version without re-applying the baseline DDL, then any
  pending forward migrations are applied in order.
- **Idempotent:** running the migrator repeatedly is a no-op once up to date.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from stabilize.queue.sqlite.schema import queue_lookup_indexes

# Version of the baseline schema defined in schema.SCHEMA. Bump only if the
# baseline itself is ever redefined (normally you add a Migration instead).
BASELINE_VERSION = 1

_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now', 'utc'))
)
"""


@dataclass(frozen=True)
class Migration:
    """A single forward-only schema migration.

    Attributes:
        version: Strictly increasing version number greater than the baseline.
        name: Human-readable description (recorded for auditing).
        statements: Ordered DDL statements to apply for this migration. Prefer
            additive, idempotent statements (e.g. ``CREATE TABLE IF NOT EXISTS``,
            ``CREATE INDEX IF NOT EXISTS``); SQLite cannot run these inside a
            transaction-safe rollback for every DDL, so each migration should be
            written to be safe to retry.
    """

    version: int
    name: str
    statements: tuple[str, ...]


# Forward-only migrations applied *after* the baseline (version 1).
# Append new migrations here with strictly increasing version numbers, e.g.:
#     Migration(2, "add_foo_column", ("ALTER TABLE stage_executions ADD COLUMN foo TEXT",))
MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        2,
        "add_stage_claims",
        (
            """
            CREATE TABLE IF NOT EXISTS stage_claims (
                execution_id TEXT NOT NULL,
                claim_key TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                claimed_at TEXT NOT NULL DEFAULT (datetime('now', 'utc')),
                PRIMARY KEY (execution_id, claim_key)
            )
            """,
        ),
    ),
    Migration(
        3,
        "add_default_queue_dlq_and_processed_execution_index",
        (
            """
            CREATE TABLE IF NOT EXISTS queue_messages_dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_id INTEGER,
                message_id TEXT NOT NULL,
                message_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                attempts INTEGER,
                error TEXT,
                last_error_at TEXT,
                created_at TEXT DEFAULT (datetime('now', 'utc')),
                moved_at TEXT DEFAULT (datetime('now', 'utc'))
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_queue_messages_dlq_type ON queue_messages_dlq(message_type)",
            "CREATE INDEX IF NOT EXISTS idx_processed_messages_execution ON processed_messages(execution_id)",
        ),
    ),
    Migration(
        4,
        "add_workflow_queue_table_and_advance_queue_ids",
        (
            "ALTER TABLE pipeline_executions ADD COLUMN queue_table TEXT",
            """
            INSERT INTO sqlite_sequence(name, seq)
            SELECT 'queue_messages', 0
            WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'queue_messages')
            """,
            """
            UPDATE sqlite_sequence SET seq = MAX(
                seq,
                COALESCE(
                    (SELECT MAX(CAST(message_id AS INTEGER)) FROM processed_messages
                     WHERE message_id GLOB '[0-9]*' AND message_id NOT GLOB '*[^0-9]*'),
                    0
                )
            )
            WHERE name = 'queue_messages'
            """,
        ),
    ),
    Migration(
        5,
        "add_workflow_config_version",
        ("ALTER TABLE pipeline_executions ADD COLUMN config_version TEXT",),
    ),
    Migration(
        6,
        "add_purge_and_lookup_indexes",
        (
            "CREATE INDEX IF NOT EXISTS idx_execution_end_time ON pipeline_executions(end_time) "
            "WHERE end_time IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_execution_application_end_time "
            "ON pipeline_executions(application, end_time) WHERE end_time IS NOT NULL",
            *queue_lookup_indexes("queue_messages"),
        ),
    ),
)


def ensure_version_table(conn: sqlite3.Connection) -> None:
    """Create the schema_migrations bookkeeping table if it does not exist."""
    conn.execute(_VERSION_TABLE)


def get_schema_version(conn: sqlite3.Connection) -> int:
    """Return the highest applied schema version.

    Returns 0 for a database that has never been stamped (e.g. an existing,
    pre-migration-framework database, or a brand new connection).
    """
    ensure_version_table(conn)
    row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def _validate_migrations(migrations: tuple[Migration, ...], baseline_version: int) -> None:
    """Validate developer-supplied migrations fail fast on misconfiguration."""
    seen: set[int] = set()
    for migration in migrations:
        if migration.version <= baseline_version:
            raise ValueError(
                f"Migration version {migration.version} ({migration.name!r}) must be "
                f"greater than the baseline version {baseline_version}"
            )
        if migration.version in seen:
            raise ValueError(f"Duplicate migration version {migration.version}")
        seen.add(migration.version)


def _current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'").fetchone()
    if row is None:
        return 0
    value = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    return int(value[0] or 0) if value else 0


def apply_migrations(
    conn: sqlite3.Connection,
    migrations: tuple[Migration, ...] = MIGRATIONS,
    baseline_version: int = BASELINE_VERSION,
) -> int:
    """Bring the database schema up to date and return the resulting version.

    This must be called *after* the baseline schema has been created (see
    :func:`stabilize.persistence.sqlite.schema.create_tables`). It stamps the
    baseline version for un-versioned databases, then applies any forward
    migrations whose version exceeds the current version, in ascending order.

    Args:
        conn: An open SQLite connection.
        migrations: Forward migrations to apply (defaults to :data:`MIGRATIONS`).
        baseline_version: The version represented by the baseline schema.

    Returns:
        The schema version after migration.
    """
    _validate_migrations(migrations, baseline_version)
    latest = max([baseline_version, *(m.version for m in migrations)])
    if _current_version(conn) >= latest:
        return latest
    if conn.in_transaction:
        conn.commit()
    conn.execute("BEGIN IMMEDIATE")
    try:
        ensure_version_table(conn)
        current = get_schema_version(conn)
        if current < baseline_version:
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, name) VALUES (?, ?)",
                (baseline_version, "baseline"),
            )
            current = baseline_version
        for migration in sorted(migrations, key=lambda m: m.version):
            if migration.version <= current:
                continue
            for statement in migration.statements:
                stmt = statement.strip()
                if stmt:
                    conn.execute(stmt)
            conn.execute(
                "INSERT INTO schema_migrations(version, name) VALUES (?, ?)",
                (migration.version, migration.name),
            )
            current = migration.version
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return current
