"""Tests for the SQLite schema migration framework.

These cover the no-regression guarantees: new databases get stamped at the
baseline, existing un-versioned databases upgrade in place without re-running
baseline DDL, and forward migrations apply once, in order, idempotently.
"""

from __future__ import annotations

import sqlite3

import pytest

from stabilize.persistence.sqlite.migrations import (
    BASELINE_VERSION,
    Migration,
    apply_migrations,
    ensure_version_table,
    get_schema_version,
)
from stabilize.persistence.sqlite.schema import SCHEMA, create_tables


def _raw_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def _install_baseline_only(conn: sqlite3.Connection) -> None:
    """Install the baseline schema the *old* way (no version stamping)."""
    for statement in SCHEMA.split(";"):
        stmt = statement.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()


class TestBaselineStamping:
    def test_create_tables_stamps_baseline(self) -> None:
        conn = _raw_conn()
        create_tables(conn)
        assert get_schema_version(conn) == BASELINE_VERSION

    def test_create_tables_still_creates_core_tables(self) -> None:
        """No regression: all core tables exist after create_tables."""
        conn = _raw_conn()
        create_tables(conn)
        names = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        for expected in (
            "pipeline_executions",
            "stage_executions",
            "task_executions",
            "queue_messages",
            "processed_messages",
            "schema_migrations",
        ):
            assert expected in names

    def test_apply_migrations_idempotent(self) -> None:
        conn = _raw_conn()
        create_tables(conn)
        v1 = apply_migrations(conn)
        v2 = apply_migrations(conn)
        assert v1 == v2 == BASELINE_VERSION
        # Exactly one baseline row recorded.
        count = conn.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = ?", (BASELINE_VERSION,)
        ).fetchone()[0]
        assert count == 1


class TestExistingDatabaseUpgrade:
    def test_existing_unversioned_db_is_stamped_without_data_loss(self) -> None:
        """An existing pre-framework DB is stamped at baseline, data preserved."""
        conn = _raw_conn()
        _install_baseline_only(conn)

        # Simulate existing customer data.
        conn.execute(
            "INSERT INTO pipeline_executions(id, type, application, status) "
            "VALUES ('p1', 'PIPELINE', 'app', 'RUNNING')"
        )
        conn.commit()

        # Un-versioned database reports version 0.
        assert get_schema_version(conn) == 0

        # Upgrade in place.
        version = apply_migrations(conn)
        assert version == BASELINE_VERSION
        assert get_schema_version(conn) == BASELINE_VERSION

        # Data preserved (baseline DDL was NOT re-run destructively).
        row = conn.execute("SELECT id, status FROM pipeline_executions").fetchone()
        assert row[0] == "p1"
        assert row[1] == "RUNNING"

    def test_pending_forward_migration_applies_to_existing_db(self) -> None:
        conn = _raw_conn()
        _install_baseline_only(conn)
        migrations = (
            Migration(2, "add_audit_table", ("CREATE TABLE audit_demo (id TEXT PRIMARY KEY)",)),
        )
        version = apply_migrations(conn, migrations=migrations)
        assert version == 2
        # baseline + v2 both recorded
        versions = {r[0] for r in conn.execute("SELECT version FROM schema_migrations").fetchall()}
        assert versions == {BASELINE_VERSION, 2}
        # new table exists
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_demo'"
        ).fetchone()


class TestForwardMigrations:
    def test_forward_migration_applies_and_records(self) -> None:
        conn = _raw_conn()
        create_tables(conn)
        migrations = (
            Migration(2, "add_col", ("ALTER TABLE task_executions ADD COLUMN demo_col TEXT",)),
        )
        version = apply_migrations(conn, migrations=migrations)
        assert version == 2
        cols = {r[1] for r in conn.execute("PRAGMA table_info(task_executions)").fetchall()}
        assert "demo_col" in cols

    def test_forward_migration_is_idempotent(self) -> None:
        conn = _raw_conn()
        create_tables(conn)
        migrations = (
            Migration(2, "add_table", ("CREATE TABLE demo2 (id TEXT PRIMARY KEY)",)),
        )
        apply_migrations(conn, migrations=migrations)
        # Second run must NOT re-run the (non-idempotent) CREATE TABLE.
        version = apply_migrations(conn, migrations=migrations)
        assert version == 2
        count = conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 2").fetchone()[0]
        assert count == 1

    def test_migrations_applied_in_order(self) -> None:
        conn = _raw_conn()
        create_tables(conn)
        migrations = (
            Migration(3, "third", ("CREATE TABLE m3 (id TEXT)",)),
            Migration(2, "second", ("CREATE TABLE m2 (id TEXT)",)),
        )
        version = apply_migrations(conn, migrations=migrations)
        assert version == 3
        ordered = [r[0] for r in conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()]
        assert ordered == [BASELINE_VERSION, 2, 3]


class TestMigrationValidation:
    def test_version_must_exceed_baseline(self) -> None:
        conn = _raw_conn()
        create_tables(conn)
        bad = (Migration(BASELINE_VERSION, "dup_baseline", ("SELECT 1",)),)
        with pytest.raises(ValueError):
            apply_migrations(conn, migrations=bad)

    def test_duplicate_version_rejected(self) -> None:
        conn = _raw_conn()
        create_tables(conn)
        bad = (
            Migration(2, "a", ("SELECT 1",)),
            Migration(2, "b", ("SELECT 1",)),
        )
        with pytest.raises(ValueError):
            apply_migrations(conn, migrations=bad)

    def test_ensure_version_table_is_idempotent(self) -> None:
        conn = _raw_conn()
        ensure_version_table(conn)
        ensure_version_table(conn)
        assert get_schema_version(conn) == 0
