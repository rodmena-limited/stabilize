"""The tenant column and tenancy-mode record reach every engine table on both backends (WI-2 foundation)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.cli.adoption import models_for
from stabilize.cli.grants import engine_tables
from stabilize.cli.legacy_history import KNOWN_LEGACY_CHECKSUMS
from stabilize.cli.migrations import get_migrations
from stabilize.cli.schema_model import model_migration
from tests.mg_support import output, scratch_database, stabilize

SHIPPED = dict(get_migrations())
ENGINE_TABLES = engine_tables(get_migrations())
TENANT_FILES = sorted(name for name, text in SHIPPED.items() if "stabilize_tenant" in text)
MODE_TABLE = "stabilize_tenancy"


def test_twelve_engine_tables_and_no_mode_table_among_them() -> None:
    assert len(ENGINE_TABLES) == 12, ENGINE_TABLES
    assert MODE_TABLE not in ENGINE_TABLES, "the mode record must never be treated as an engine table"
    assert MODE_TABLE in "".join(SHIPPED.values()), "no shipped migration creates the mode table"


def test_a_shipped_migration_adds_the_tenant_column_to_every_engine_table() -> None:
    assert TENANT_FILES, "no shipped migration mentions stabilize_tenant"
    text = "\n".join(SHIPPED[name] for name in TENANT_FILES)
    for table in ENGINE_TABLES:
        assert f"ALTER TABLE {table} ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT ''" in text, table
    assert "IF NOT EXISTS stabilize_tenant" not in text, "a pre-existing host column must fail loudly, not be adopted"


def test_the_tenant_migrations_are_fully_modelled_and_recorded() -> None:
    for name in TENANT_FILES:
        model = model_migration(name, SHIPPED[name])
        assert not model.unmodelled, f"{name} has statements adoption cannot check: {model.unmodelled}"
        assert KNOWN_LEGACY_CHECKSUMS.get(name), f"{name} is not in KNOWN_LEGACY_CHECKSUMS"
    tenant_columns = {
        (e.relation, e.name) for m in models_for(SHIPPED) for e in m.expectations if e.name == "stabilize_tenant"
    }
    assert tenant_columns == {(t, "stabilize_tenant") for t in ENGINE_TABLES}


def test_schema_model_accepts_set_local_and_create_function_and_trigger() -> None:
    text = (
        "-- migrate: up\n"
        "SET LOCAL lock_timeout = '5s';\n"
        "CREATE OR REPLACE FUNCTION stabilize_is_table_owner(rel regclass) RETURNS boolean "
        "LANGUAGE sql STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS $$ "
        "SELECT pg_has_role(current_user, c.relowner, 'MEMBER') FROM pg_catalog.pg_class c WHERE c.oid = rel $$;\n"
        "CREATE TRIGGER t BEFORE UPDATE ON pipeline_executions FOR EACH ROW EXECUTE FUNCTION f();\n"
        "-- migrate: down\nSELECT 1;\n"
    )
    model = model_migration("x.sql", text)
    assert model.unmodelled == [], model.unmodelled
    kinds = {e.kind for e in model.expectations}
    assert {"function", "trigger"} <= kinds, kinds


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgtenant") as url:
        yield url


def test_mg_up_leaves_the_column_and_the_seeded_mode_row_in_postgres(db: str) -> None:
    psycopg = pytest.importorskip("psycopg")
    result = stabilize("mg-up", "--db-url", db)
    assert result.returncode == 0, output(result)
    with psycopg.connect(db) as conn:
        rows = conn.execute(
            "SELECT table_name, data_type, is_nullable, column_default FROM information_schema.columns "
            "WHERE column_name = 'stabilize_tenant' AND table_schema = 'public' ORDER BY table_name"
        ).fetchall()
        by_table = {r[0]: (r[1], r[2], r[3]) for r in rows}
        assert set(by_table) == set(ENGINE_TABLES), set(ENGINE_TABLES) ^ set(by_table)
        assert set(by_table.values()) == {("text", "NO", "''::text")}, by_table
        mode = conn.execute(f"SELECT id, mode FROM {MODE_TABLE}").fetchall()
        assert mode == [(1, "single")], mode
        owner_fn = conn.execute(
            "SELECT provolatile, prosecdef FROM pg_proc WHERE proname = 'stabilize_is_table_owner'"
        ).fetchone()
        assert owner_fn == ("s", False), owner_fn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def test_sqlite_store_adds_the_column_to_every_table_it_owns_at_first_use(tmp_path: Path) -> None:
    from stabilize.persistence.sqlite import SqliteWorkflowStore
    from stabilize.persistence.sqlite.migrations import apply_migrations
    from stabilize.persistence.sqlite.schema import SCHEMA
    from stabilize.persistence.sqlite.signals import SIGNALS_SCHEMA
    from stabilize.queue.sqlite import create_queue_tables

    path = tmp_path / "old.db"
    with sqlite3.connect(path) as raw:
        raw.executescript(SCHEMA)
        raw.executescript(SIGNALS_SCHEMA)
        apply_migrations(raw)
        create_queue_tables(raw, "queue_messages")
        create_queue_tables(raw, "custom_queue")
        for table in ("pipeline_executions", "queue_messages", "custom_queue", "workflow_signals"):
            assert "stabilize_tenant" not in {row[1] for row in raw.execute(f"PRAGMA table_info({table})")}, (
                f"the fixture already has the column on {table}; the test would prove nothing"
            )
    store = SqliteWorkflowStore(f"sqlite:///{path}", create_tables=False)
    try:
        conn = store._get_connection()
        for table in (
            "pipeline_executions",
            "stage_executions",
            "task_executions",
            "queue_messages",
            "queue_messages_dlq",
            "processed_messages",
            "stage_claims",
            "custom_queue",
            "custom_queue_dlq",
            "workflow_signals",
        ):
            assert table in _tables(conn), f"{table} missing after first use"
            assert "stabilize_tenant" in _columns(conn, table), f"{table} has no stabilize_tenant column"
        assert MODE_TABLE in _tables(conn)
        assert conn.execute(f"SELECT mode FROM {MODE_TABLE}").fetchone()[0] == "single"
    finally:
        store.close()


def test_sqlite_event_store_file_gets_the_column_and_the_mode_table(tmp_path: Path) -> None:
    from stabilize.events.store.sqlite import SqliteEventStore
    from stabilize.events.store.sqlite.schema import EVENTS_SCHEMA, SNAPSHOTS_SCHEMA, SUBSCRIPTIONS_SCHEMA

    path = tmp_path / "events-old.db"
    with sqlite3.connect(path) as raw:
        raw.executescript(EVENTS_SCHEMA + SNAPSHOTS_SCHEMA + SUBSCRIPTIONS_SCHEMA)
    from stabilize.persistence.connection import get_connection_manager

    url = f"sqlite:///{path}"
    store = SqliteEventStore(url, create_tables=False)
    try:
        conn = store._get_connection()
        for table in ("events", "snapshots", "event_subscriptions"):
            assert "stabilize_tenant" in _columns(conn, table), f"{table} has no stabilize_tenant column"
        assert MODE_TABLE in _tables(conn)
    finally:
        get_connection_manager().close_sqlite_connection(url)
