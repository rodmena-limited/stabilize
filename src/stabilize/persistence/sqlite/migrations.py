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
MIGRATIONS: tuple[Migration, ...] = ()


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
    ensure_version_table(conn)
    current = get_schema_version(conn)

    # Stamp the baseline for an existing or just-created un-versioned database so
    # the baseline DDL is never re-run on upgrade.
    if current < baseline_version:
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, name) VALUES (?, ?)",
            (baseline_version, "baseline"),
        )
        current = baseline_version

    # Apply forward migrations strictly in version order.
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
    return current
