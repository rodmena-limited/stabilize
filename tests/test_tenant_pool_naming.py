"""application_name on engine pools, INVALID index refusal in mg-up, and the event store's create_tables path."""

from __future__ import annotations

import socket
from collections.abc import Iterator
from typing import Any

import pytest

from stabilize import __version__
from stabilize.persistence.tenancy import EngineSchemaError
from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgname") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


def _application_name(store: Any) -> str:
    with store.transaction() as txn:
        row = txn._conn.execute("SHOW application_name").fetchone()
    return str(row["application_name"] if isinstance(row, dict) else row[0])


def test_engine_pools_name_their_connections_unless_the_host_did(db: str) -> None:
    from stabilize.persistence.pool_options import PoolOptions
    from stabilize.persistence.postgres import PostgresWorkflowStore

    store = PostgresWorkflowStore(db)
    try:
        assert _application_name(store) == f"stabilize/{__version__} {socket.gethostname()}"
    finally:
        store.close()

    named = PostgresWorkflowStore(db, options=PoolOptions(connect_kwargs={"application_name": "host-app"}))
    try:
        assert _application_name(named) == "host-app"
    finally:
        named.close()

    via_dsn = PostgresWorkflowStore(db + "?application_name=dsn-app")
    try:
        assert _application_name(via_dsn) == "dsn-app"
    finally:
        via_dsn.close()


def test_mg_up_refuses_an_invalid_engine_index_and_names_the_reindex(db: str) -> None:
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(
            "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'idx_executions_tenant_status'::regclass"
        )
    try:
        result = stabilize("mg-up", "--db-url", db)
        assert result.returncode == 1, output(result)
        assert "idx_executions_tenant_status" in output(result), output(result)
        assert "REINDEX INDEX" in output(result), output(result)
    finally:
        with psycopg.connect(db, autocommit=True) as conn:
            conn.execute("REINDEX INDEX idx_executions_tenant_status")
    assert stabilize("mg-up", "--db-url", db).returncode == 0


def test_the_engine_warns_about_an_invalid_index_and_carries_on(db: str, caplog: pytest.LogCaptureFixture) -> None:
    import logging

    from stabilize.persistence.postgres import PostgresWorkflowStore

    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(
            "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'idx_stages_tenant_status_id'::regclass"
        )
    store = PostgresWorkflowStore(db)
    try:
        with caplog.at_level(logging.WARNING):
            assert store.count_by_application("nobody") == 0
        assert any("idx_stages_tenant_status_id" in r.getMessage() for r in caplog.records), caplog.text
    finally:
        store.close()
        with psycopg.connect(db, autocommit=True) as conn:
            conn.execute("REINDEX INDEX idx_stages_tenant_status_id")


def test_create_tables_leaves_present_tables_alone_and_the_check_names_the_ddl(db: str) -> None:
    from stabilize.events.store.postgres.store import PostgresEventStore

    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("ALTER TABLE snapshots DROP COLUMN stabilize_tenant")
    with pytest.warns(DeprecationWarning):
        store = PostgresEventStore(db, create_tables=True)
    try:
        with psycopg.connect(db) as conn:
            columns = {
                r[0]
                for r in conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'snapshots'"
                )
            }
        assert "stabilize_tenant" not in columns, "create_tables altered a present table"
        with pytest.raises(EngineSchemaError, match="ALTER TABLE snapshots ADD COLUMN stabilize_tenant"):
            store.count_events()
    finally:
        store.close()
