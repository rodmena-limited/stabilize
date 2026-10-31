"""The armed borrow path and the PostgreSQL fence installed by mg-tenancy enable (WI-2 slice 4)."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pytest

from stabilize.models.workflow import Workflow
from stabilize.persistence.pool_options import PoolOptions
from stabilize.persistence.tenancy import (
    TenancyModeChanged,
    TenantContextError,
    TenantMismatchError,
    TenantPermissionError,
    tenant_error_from,
)
from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")

ENGINE_TABLES = (
    "pipeline_executions",
    "stage_executions",
    "task_executions",
    "queue_messages",
    "queue_messages_dlq",
    "processed_messages",
    "workflow_signals",
    "stage_claims",
    "events",
    "snapshots",
    "event_subscriptions",
    "task_leases",
)


@pytest.fixture(scope="module")
def tenancy_db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgfence") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
        assert enabled.returncode == 0, output(enabled)
        yield url


@pytest.fixture()
def fresh_db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgfence2") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


def _setting(conn: Any, name: str) -> str | None:
    row = conn.execute("SELECT current_setting(%s, true) AS v", (name,)).fetchone()
    return row["v"] if isinstance(row, dict) else row[0]


def test_every_engine_borrow_arms_the_tenant_setting_and_re_arms_after_a_commit(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    store = PostgresWorkflowStore(tenancy_db, tenant="acme")
    try:
        with store.transaction() as txn:
            conn = txn._conn
            assert _setting(conn, "stabilize.tenant") == "acme"
            conn.commit()
            assert _setting(conn, "stabilize.tenant") == "acme", "the setting was not re-armed after a commit"
            conn.rollback()
            assert _setting(conn, "stabilize.tenant") == "acme", "the setting was not re-armed after a rollback"
    finally:
        store.close()


def test_a_connection_that_arrives_with_the_setting_already_set_is_refused(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    def preset(conn: Any) -> None:
        conn.execute("SELECT set_config('stabilize.tenant', 'intruder', false)")
        conn.commit()

    store = PostgresWorkflowStore(
        tenancy_db, tenant="acme", options=PoolOptions(min_size=0, max_size=1, on_borrow=preset)
    )
    try:
        with pytest.raises(TenantContextError, match="intruder"):
            store.count_by_application("nobody")
    finally:
        store.close()


def test_the_host_tenant_setting_is_set_per_transaction_and_a_foreign_value_is_refused(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    options = PoolOptions(min_size=0, max_size=1, tenant_setting="app.current_tenant")
    store = PostgresWorkflowStore(tenancy_db, tenant="acme", options=options)
    try:
        with store.transaction() as txn:
            assert _setting(txn._conn, "app.current_tenant") == "acme"
    finally:
        store.close()

    def foreign(conn: Any) -> None:
        conn.execute("SELECT set_config('app.current_tenant', 'globex', false)")
        conn.commit()

    hostile = PoolOptions(min_size=0, max_size=1, tenant_setting="app.current_tenant", on_borrow=foreign)
    store = PostgresWorkflowStore(tenancy_db, tenant="acme", options=hostile)
    try:
        with pytest.raises(TenantContextError, match="globex"):
            store.count_by_application("nobody")
    finally:
        store.close()


def test_an_autocommit_connection_is_refused(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    def autocommit(conn: Any) -> None:
        conn.autocommit = True

    store = PostgresWorkflowStore(
        tenancy_db, tenant="acme", options=PoolOptions(min_size=0, max_size=1, on_borrow=autocommit)
    )
    try:
        with pytest.raises(TenantContextError, match="autocommit"):
            store.count_by_application("nobody")
    finally:
        store.close()


def _fence_state(url: str) -> dict[str, Any]:
    with psycopg.connect(url) as conn:
        triggers = {
            r[0]
            for r in conn.execute(
                "SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                "WHERE t.tgname = 'stabilize_tenant_fence' AND NOT t.tgisinternal"
            )
        }
        checks = {
            r[0]
            for r in conn.execute(
                "SELECT c.relname FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid "
                "WHERE k.conname = 'stabilize_tenant_nonempty'"
            )
        }
        defaults = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT table_name, column_default FROM information_schema.columns "
                "WHERE table_schema = 'public' AND column_name = 'stabilize_tenant'"
            )
        }
        fn = conn.execute(
            "SELECT prosecdef, proconfig FROM pg_proc WHERE proname = 'stabilize_tenant_fence'"
        ).fetchone()
    return {"triggers": triggers, "checks": checks, "defaults": defaults, "function": fn}


def test_enable_installs_the_fence_and_check_and_drops_the_default_and_disable_undoes_it(fresh_db: str) -> None:
    before = _fence_state(fresh_db)
    assert before["triggers"] == set() and before["checks"] == set() and before["function"] is None
    assert all(default == "''::text" for default in before["defaults"].values()), before["defaults"]

    enabled = stabilize("mg-tenancy", "enable", "--db-url", fresh_db)
    assert enabled.returncode == 0, output(enabled)
    after = _fence_state(fresh_db)
    assert after["triggers"] == set(ENGINE_TABLES), after["triggers"] ^ set(ENGINE_TABLES)
    assert after["checks"] == set(ENGINE_TABLES)
    assert all(default is None for default in after["defaults"].values()), after["defaults"]
    assert after["function"] is not None
    assert after["function"][0] is False, "the fence function must be SECURITY INVOKER"
    assert any(c.startswith("search_path=") for c in (after["function"][1] or [])), after["function"]

    disabled = stabilize("mg-tenancy", "disable", "--db-url", fresh_db)
    assert disabled.returncode == 0, output(disabled)
    restored = _fence_state(fresh_db)
    assert restored["triggers"] == set() and restored["checks"] == set() and restored["function"] is None
    assert all(default == "''::text" for default in restored["defaults"].values()), restored["defaults"]


def _insert(conn: Any, tenant: str, wf_id: str) -> None:
    conn.execute(
        "INSERT INTO pipeline_executions (id, type, application, status, stabilize_tenant) "
        "VALUES (%s, 'PIPELINE', 'fence', 'RUNNING', %s)",
        (wf_id, tenant),
    )


def _sqlstate_of(callable_: Any) -> tuple[str | None, Exception]:
    try:
        callable_()
    except psycopg.Error as exc:
        return exc.sqlstate, exc
    raise AssertionError("the statement was not refused")


def test_the_fence_refuses_unset_wrong_and_cross_tenant_writes(tenancy_db: str) -> None:
    wf_id = f"01FENCE{uuid.uuid4().hex[:19].upper()}"
    with psycopg.connect(tenancy_db) as conn:
        state, exc = _sqlstate_of(lambda: _insert(conn, "acme", wf_id))
        assert state == "ST001", state
        assert isinstance(tenant_error_from(exc), TenancyModeChanged)
        conn.rollback()

        conn.execute("SELECT set_config('stabilize.tenant', 'globex', true)")
        state, exc = _sqlstate_of(lambda: _insert(conn, "acme", wf_id))
        assert state == "ST002", state
        assert isinstance(tenant_error_from(exc), TenantMismatchError)
        conn.rollback()

        conn.execute("SELECT set_config('stabilize.tenant', 'acme', true)")
        _insert(conn, "acme", wf_id)
        state, _ = _sqlstate_of(
            lambda: conn.execute("UPDATE pipeline_executions SET stabilize_tenant = 'globex' WHERE id = %s", (wf_id,))
        )
        assert state == "ST002", state
        conn.rollback()

        conn.execute("SELECT set_config('stabilize.tenant', 'acme', true)")
        _insert(conn, "acme", wf_id)
        conn.commit()

        conn.execute("SELECT set_config('stabilize.tenant', 'globex', true)")
        state, _ = _sqlstate_of(lambda: conn.execute("DELETE FROM pipeline_executions WHERE id = %s", (wf_id,)))
        assert state == "ST002", state
        conn.rollback()

        conn.execute("SELECT set_config('stabilize.tenant', 'acme', true)")
        conn.execute("DELETE FROM pipeline_executions WHERE id = %s", (wf_id,))
        conn.commit()

        state, _ = _sqlstate_of(lambda: _insert(conn, "", wf_id))
        assert state in ("ST001", "23514"), state
        conn.rollback()


def test_the_star_setting_passes_for_the_owner_and_is_refused_for_others(tenancy_db: str) -> None:
    wf_id = f"01STAR{uuid.uuid4().hex[:20].upper()}"
    role, password = f"fence_{uuid.uuid4().hex[:8]}", "fence-pw"
    parts = urlsplit(tenancy_db)
    with psycopg.connect(tenancy_db, autocommit=True) as owner:
        owner.execute("SELECT set_config('stabilize.tenant', '*', false)")
        _insert(owner, "acme", wf_id)
        owner.execute("DELETE FROM pipeline_executions WHERE id = %s", (wf_id,))
        owner.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        owner.execute(f'GRANT USAGE ON SCHEMA public TO "{role}"')
        owner.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{role}"')
        owner.execute(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{role}"')
    other = urlunsplit((parts.scheme, f"{role}:{password}@{parts.hostname}:{parts.port}", parts.path, "", ""))
    try:
        with psycopg.connect(other) as conn:
            conn.execute("SELECT set_config('stabilize.tenant', '*', true)")
            state, exc = _sqlstate_of(lambda: _insert(conn, "acme", wf_id))
            assert state == "ST003", state
            assert isinstance(tenant_error_from(exc), TenantPermissionError)
    finally:
        with psycopg.connect(tenancy_db, autocommit=True) as owner:
            owner.execute(f'DROP OWNED BY "{role}"')
            owner.execute(f'DROP ROLE IF EXISTS "{role}"')


def test_engine_writes_pass_the_fence(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.messages import StartWorkflow
    from stabilize.queue.postgres import PostgresQueue

    store = PostgresWorkflowStore(tenancy_db, tenant="acme")
    queue = PostgresQueue(tenancy_db, tenant="acme")
    try:
        wf = Workflow.create(application="fenced", name="w", stages=[])
        store.store(wf)
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id=wf.id))
        assert store.retrieve(wf.id).id == wf.id
        message = queue.poll_one()
        assert message is not None
        queue.ack(message)
        store.delete(wf.id)
    finally:
        queue.close()
        store.close()


def test_enable_refuses_force_rls_and_a_preset_setting(fresh_db: str) -> None:
    with psycopg.connect(fresh_db, autocommit=True) as conn:
        conn.execute("ALTER TABLE stage_executions ENABLE ROW LEVEL SECURITY")
        conn.execute("ALTER TABLE stage_executions FORCE ROW LEVEL SECURITY")
        conn.execute("CREATE POLICY host_all ON stage_executions FOR ALL USING (true) WITH CHECK (true)")
    refused = stabilize("mg-tenancy", "enable", "--db-url", fresh_db)
    assert refused.returncode == 1, output(refused)
    assert "stage_executions" in output(refused) and "host_all" in output(refused), output(refused)
    assert _fence_state(fresh_db)["triggers"] == set()

    dbname = urlsplit(fresh_db).path.lstrip("/")
    with psycopg.connect(fresh_db, autocommit=True) as conn:
        conn.execute("ALTER TABLE stage_executions NO FORCE ROW LEVEL SECURITY")
        conn.execute("ALTER TABLE stage_executions DISABLE ROW LEVEL SECURITY")
        conn.execute("DROP POLICY host_all ON stage_executions")
        conn.execute(f"ALTER DATABASE \"{dbname}\" SET stabilize.tenant = 'preset'")
    try:
        refused = stabilize("mg-tenancy", "enable", "--db-url", fresh_db)
        assert refused.returncode == 1, output(refused)
        assert "stabilize.tenant" in output(refused) and "preset" in output(refused), output(refused)
    finally:
        with psycopg.connect(fresh_db, autocommit=True) as conn:
            conn.execute(f'ALTER DATABASE "{dbname}" RESET stabilize.tenant')


def test_enable_ignores_a_tenant_setting_on_another_database(fresh_db: str, postgres_container: Any) -> None:
    with scratch_database(postgres_container, "mgother") as other:
        other_name = urlsplit(other).path.lstrip("/")
        with psycopg.connect(other, autocommit=True) as conn:
            conn.execute(f"ALTER DATABASE \"{other_name}\" SET stabilize.tenant = 'elsewhere'")
        try:
            enabled = stabilize("mg-tenancy", "enable", "--db-url", fresh_db)
            assert enabled.returncode == 0, output(enabled)
            assert _fence_state(fresh_db)["triggers"], "enable did not install the fence"
        finally:
            with psycopg.connect(other, autocommit=True) as conn:
                conn.execute(f'ALTER DATABASE "{other_name}" RESET stabilize.tenant')
