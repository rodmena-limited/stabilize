"""The tenant parameter: validation, equality checks between parts, and the mode record (WI-2 API)."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import fields
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pytest

from stabilize.persistence.pool_options import PoolOptions
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.persistence.tenancy import TenancyModeError, TenantError, validate_tenant
from stabilize.queue.sqlite import SqliteQueue
from tests.mg_support import output, scratch_database, stabilize


@pytest.mark.parametrize("tenant", ["", "acme", "acme.eu", "t:1", "a-b", "x" * 64])
def test_well_formed_tenants_are_accepted(tenant: str) -> None:
    assert validate_tenant(tenant) == tenant


@pytest.mark.parametrize("tenant", [None, "x" * 65, "a_b", "a b", "a\x1fb", "*", "ACME/1", "é"])
def test_ill_formed_tenants_are_refused(tenant: Any) -> None:
    with pytest.raises(TenantError):
        validate_tenant(tenant)


def test_tenant_setting_and_audit_key_are_part_of_the_pool_key() -> None:
    base = PoolOptions()
    assert PoolOptions(tenant_setting="app.current_tenant").key() != base.key()
    assert PoolOptions(audit_key=b"k1").key() != PoolOptions(audit_key=b"k2").key()
    declared_instance_read: set[str] = set()
    keyed = {f.name for f in fields(PoolOptions)} - declared_instance_read
    assert len(base.key()) == len(keyed), f"PoolOptions.key() covers {len(base.key())} of {len(keyed)} fields"


@pytest.mark.parametrize("setting", ["role", "stabilize.tenant", "stabilize.x", "App.Tenant", "no_dot", "a.b.c"])
def test_tenant_setting_must_be_a_custom_guc_outside_the_stabilize_class(setting: str) -> None:
    with pytest.raises(ValueError, match="tenant_setting"):
        PoolOptions(tenant_setting=setting)


def test_sqlite_store_and_queue_carry_and_check_their_tenant(tmp_path: Path) -> None:
    from stabilize.orchestrator import Orchestrator
    from stabilize.queue.processor import QueueProcessor

    url = f"sqlite:///{tmp_path / 'one.db'}"
    store = SqliteWorkflowStore(url, create_tables=True)
    assert store.tenant == ""
    queue = SqliteQueue(url)
    assert queue.tenant == ""
    with pytest.raises(TenantError):
        SqliteQueue(url, tenant=None)  # type: ignore[arg-type]
    try:
        QueueProcessor(queue, store=store)
        Orchestrator(queue, store=store)
    finally:
        store.close()
        queue.close()


def test_parts_with_different_tenants_are_refused_together(tmp_path: Path) -> None:
    from stabilize.orchestrator import Orchestrator
    from stabilize.queue.processor import QueueProcessor

    url = f"sqlite:///{tmp_path / 'two.db'}"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    try:
        foreign = SqliteQueue.__new__(SqliteQueue)
        foreign.__dict__.update(queue.__dict__)
        foreign.tenant = "other"
        with pytest.raises(TenantError, match="tenant"):
            QueueProcessor(foreign, store=store)
        with pytest.raises(TenantError, match="tenant"):
            Orchestrator(foreign, store=store)
        with pytest.raises(TenantError, match="tenant"), store.transaction(foreign):
            pass
    finally:
        store.close()
        queue.close()


def test_single_tenant_mode_refuses_a_named_tenant_on_sqlite(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'single.db'}"
    SqliteWorkflowStore(url, create_tables=True).close()
    with pytest.raises(TenancyModeError, match="single-tenant"):
        SqliteWorkflowStore(url, tenant="acme")


psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgapi") as url:
        yield url


SOME_ID = "01JLIVEPROBE0000000000RUN0"


def test_single_tenant_mode_refuses_a_named_tenant_on_postgres_at_first_use(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    assert stabilize("mg-up", "--db-url", db).returncode == 0, output(stabilize("mg-status", "--db-url", db))
    store = PostgresWorkflowStore(db, tenant="acme")
    queue = PostgresQueue(db, tenant="acme")
    try:
        assert store.tenant == "acme" and queue.tenant == "acme"
        with pytest.raises(TenancyModeError, match="single-tenant"):
            store.retrieve(SOME_ID)
        with pytest.raises(TenancyModeError, match="single-tenant"):
            queue.poll_one()
    finally:
        queue.close()
        store.close()
    plain = PostgresWorkflowStore(db)
    try:
        assert plain.tenant == ""
        assert plain.count_by_application("nobody") == 0
    finally:
        plain.close()


def test_a_mode_table_with_no_row_is_refused_not_defaulted(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    assert stabilize("mg-up", "--db-url", db).returncode == 0
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("DELETE FROM stabilize_tenancy")
    store = PostgresWorkflowStore(db)
    try:
        with pytest.raises(TenancyModeError, match="no row"):
            store.count_by_application("nobody")
    finally:
        store.close()


def test_constructing_a_postgres_store_or_queue_does_not_borrow_a_connection(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    assert stabilize("mg-up", "--db-url", db).returncode == 0
    borrows: list[str] = []

    def on_borrow(conn: Any) -> None:
        borrows.append("borrow")

    options = PoolOptions(min_size=0, max_size=2, on_borrow=on_borrow)
    store = PostgresWorkflowStore(db, options=options, tenant="acme")
    queue = PostgresQueue(db, options=options, tenant="acme")
    try:
        assert borrows == [], f"construction borrowed {len(borrows)} connection(s)"
        with pytest.raises(TenancyModeError):
            store.count_by_application("nobody")
        assert borrows == ["borrow"]
    finally:
        queue.close()
        store.close()


def _runtime_role(db: str, role: str, password: str) -> str:
    from stabilize.cli.grants import engine_tables
    from stabilize.cli.migrations import get_migrations

    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        conn.execute(f'GRANT USAGE ON SCHEMA public TO "{role}"')
        for table in engine_tables(get_migrations()):
            conn.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO "{role}"')
        conn.execute(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{role}"')
    parts = urlsplit(db)
    return urlunsplit((parts.scheme, f"{role}:{password}@{parts.hostname}:{parts.port}", parts.path, "", ""))


def _drop_role(db: str, role: str) -> None:
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(f'DROP OWNED BY "{role}"')
        conn.execute(f'DROP ROLE IF EXISTS "{role}"')


def _check_grants(role: str, db: str, capsys: Any) -> tuple[int, str]:
    from stabilize.cli import commands

    with pytest.raises(SystemExit) as raised:
        commands.mg_check_grants(role, db)
    return int(raised.value.code or 0), capsys.readouterr().out


def test_a_role_without_select_on_the_mode_table_is_told_the_grant(db: str, capsys: Any) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    assert stabilize("mg-up", "--db-url", db).returncode == 0
    role = f"app_{uuid.uuid4().hex[:8]}"
    app = _runtime_role(db, role, "app-pw-102")
    store = PostgresWorkflowStore(app)
    try:
        with pytest.raises(TenancyModeError, match=f'GRANT SELECT ON stabilize_tenancy TO "{role}"'):
            store.count_by_application("nobody")
        code, out = _check_grants(role, db, capsys)
        assert code == 1, out
        assert "MISSING  SELECT on public.stabilize_tenancy" in out, out
        with psycopg.connect(db, autocommit=True) as conn:
            conn.execute(f'GRANT SELECT ON stabilize_tenancy TO "{role}"')
        assert store.count_by_application("nobody") == 0
        code, out = _check_grants(role, db, capsys)
        assert code == 0, out
    finally:
        store.close()
        _drop_role(db, role)


def test_the_event_store_creates_the_mode_table_and_tenant_column_on_a_fresh_database(db: str) -> None:
    from stabilize.events.store.postgres.schema import setup_ddl
    from stabilize.events.store.postgres.store import PostgresEventStore

    assert "stabilize_tenancy" in setup_ddl()
    with pytest.warns(DeprecationWarning):
        PostgresEventStore(db, create_tables=True).close()
    with psycopg.connect(db) as conn:
        assert conn.execute("SELECT id, mode FROM stabilize_tenancy").fetchall() == [(1, "single")]
        for table in ("events", "snapshots", "event_subscriptions"):
            row = conn.execute(
                "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
                "WHERE table_name = %s AND column_name = 'stabilize_tenant'",
                (table,),
            ).fetchone()
            assert row == ("text", "NO", "''::text"), (table, row)
    with pytest.raises(TenancyModeError, match="single-tenant"):
        PostgresEventStore(db, tenant="acme")


STORE_OPERATIONS: dict[str, Any] = {
    "store": lambda s: s.store(_workflow()),
    "retrieve": lambda s: s.retrieve(SOME_ID),
    "retrieve_execution_summary": lambda s: s.retrieve_execution_summary(SOME_ID),
    "update_status": lambda s: s.update_status(_workflow()),
    "delete": lambda s: s.delete(SOME_ID),
    "retrieve_stage": lambda s: s.retrieve_stage(SOME_ID),
    "get_upstream_stages": lambda s: s.get_upstream_stages(SOME_ID, "a"),
    "get_downstream_stages": lambda s: s.get_downstream_stages(SOME_ID, "a"),
    "get_synthetic_stages": lambda s: s.get_synthetic_stages(SOME_ID, SOME_ID),
    "get_merged_ancestor_outputs": lambda s: s.get_merged_ancestor_outputs(SOME_ID, "a"),
    "retrieve_by_pipeline_config_id": lambda s: list(s.retrieve_by_pipeline_config_id("cfg")),
    "retrieve_by_application": lambda s: list(s.retrieve_by_application("app")),
    "get_all_pending_workflows": lambda s: list(s.get_all_pending_workflows()),
    "pause": lambda s: s.pause(SOME_ID, "me"),
    "resume": lambda s: s.resume(SOME_ID),
    "cancel": lambda s: s.cancel(SOME_ID, "me", "why"),
    "is_message_processed": lambda s: s.is_message_processed("m"),
    "mark_message_processed": lambda s: s.mark_message_processed("m", "h", SOME_ID),
    "cleanup_old_processed_messages": lambda s: s.cleanup_old_processed_messages(),
    "cleanup_completed_stage_claims": lambda s: s.cleanup_completed_stage_claims(),
    "cleanup_buffered_signals": lambda s: s.cleanup_buffered_signals(),
    "get_processed_message_ids": lambda s: s.get_processed_message_ids(),
    "count_buffered_signal_stages": lambda s: s.count_buffered_signal_stages(),
    "completed_workflow_ids": lambda s: s.completed_workflow_ids(0, 10),
    "buffer_signal": lambda s: s.buffer_signal(SOME_ID, "a", "go", {}),
    "pending_signal_count": lambda s: s.pending_signal_count(SOME_ID, "a"),
    "pending_signals": lambda s: s.pending_signals(SOME_ID, "a"),
    "discard_signals": lambda s: s.discard_signals(SOME_ID, "a"),
    "retrieve_stages": lambda s: s.retrieve_stages(_stage_criteria()),
    "count_stages": lambda s: s.count_stages(_stage_criteria()),
    "transaction": lambda s: _in_transaction(s),
    "lease_acquire": lambda s: _lease(s).acquire("t"),
}


def _stage_criteria() -> Any:
    from stabilize.persistence.store import StageCriteria

    return StageCriteria(execution_id=SOME_ID)


def _in_transaction(store: Any) -> Any:
    with store.transaction() as txn:
        return txn.workflow_status(SOME_ID)


def _lease(store: Any) -> Any:
    from stabilize.persistence.task_lease import TaskLeaseManager

    return TaskLeaseManager(store)


QUEUE_OPERATIONS: dict[str, Any] = {
    "poll_one": lambda q: q.poll_one(),
    "size": lambda q: q.size(),
    "clear": lambda q: q.clear(),
    "live_delivery_keys": lambda q: q.live_delivery_keys(["1"]),
    "has_pending_message_for_task": lambda q: q.has_pending_message_for_task("t"),
    "has_pending_message_for_stage": lambda q: q.has_pending_message_for_stage("s"),
    "has_pending_message_for_execution": lambda q: q.has_pending_message_for_execution(SOME_ID),
    "purge_workflow": lambda q: q.purge_workflow(SOME_ID),
    "list_dlq": lambda q: q.list_dlq(),
    "dlq_size": lambda q: q.dlq_size(),
    "clear_dlq": lambda q: q.clear_dlq(),
    "check_and_move_expired": lambda q: q.check_and_move_expired(),
}


def _workflow() -> Any:
    from stabilize.models.workflow import Workflow

    return Workflow.create(application="tenant-api", name="n", stages=[])


@pytest.fixture(scope="module")
def single_mode_db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgadmit") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.mark.parametrize("operation", sorted(STORE_OPERATIONS))
def test_every_store_operation_is_admitted_by_the_mode_check(single_mode_db: str, operation: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    store = PostgresWorkflowStore(single_mode_db, tenant="acme")
    try:
        with pytest.raises(TenancyModeError, match="single-tenant"):
            STORE_OPERATIONS[operation](store)
    finally:
        store.close()


@pytest.mark.parametrize("operation", sorted(QUEUE_OPERATIONS))
def test_every_queue_operation_is_admitted_by_the_mode_check(single_mode_db: str, operation: str) -> None:
    from stabilize.queue.postgres import PostgresQueue

    queue = PostgresQueue(single_mode_db, tenant="acme")
    try:
        with pytest.raises(TenancyModeError, match="single-tenant"):
            QUEUE_OPERATIONS[operation](queue)
    finally:
        queue.close()
