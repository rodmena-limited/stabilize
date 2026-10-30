"""PoolOptions.check, max_idle, on_borrow and reset reach the pool and behave as documented (#100, #101)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextvars import ContextVar
from typing import Any

import pytest

from stabilize import Workflow
from stabilize.persistence.borrow_hook import BorrowHookError
from stabilize.persistence.connection import get_connection_manager
from stabilize.persistence.pool_options import PoolOptions
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")

TENANT: ContextVar[str] = ContextVar("stabilize_pool_hook_tenant", default="")


def _noop(conn: Any) -> None:
    return None


def _other(conn: Any) -> None:
    return None


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgpool") as url:
        yield url


@pytest.mark.parametrize("value", [0, -1, True, "60"])
def test_max_idle_must_be_a_positive_number_of_seconds(value: Any) -> None:
    with pytest.raises(ValueError, match="max_idle"):
        PoolOptions(max_idle=value)


@pytest.mark.parametrize("field", ["check", "on_borrow", "reset"])
def test_each_callback_is_part_of_the_pool_key(field: str) -> None:
    assert PoolOptions(**{field: _noop}).key() != PoolOptions(**{field: _other}).key()
    assert PoolOptions(**{field: _noop}).key() != PoolOptions().key()


def test_max_idle_is_part_of_the_pool_key() -> None:
    assert PoolOptions(max_idle=60).key() != PoolOptions(max_idle=600).key() != PoolOptions().key()


def test_check_reset_and_max_idle_reach_psycopg_pool(db: str) -> None:
    calls = {"check": 0, "reset": 0}

    def check(conn: Any) -> None:
        calls["check"] += 1

    def reset(conn: Any) -> None:
        calls["reset"] += 1

    manager = get_connection_manager()
    options = PoolOptions(min_size=1, max_size=2, check=check, reset=reset, max_idle=45)
    pool = manager.get_postgres_pool(db, options=options)
    try:
        with pool.connection() as conn:
            conn.execute("SELECT 1")
        deadline = time.monotonic() + 5
        while calls["reset"] == 0 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert pool.max_idle == 45
        assert calls["check"] >= 1, "check did not run when a connection was handed out"
        assert calls["reset"] >= 1, "reset did not run when the connection was returned"
    finally:
        manager.release_postgres_pool(pool)


def _set_tenant(conn: Any) -> None:
    conn.execute("SELECT set_config('app.tenant_id', %s, false)", (TENANT.get(),))
    conn.commit()


def _clear_tenant(conn: Any) -> None:
    conn.execute("SELECT set_config('app.tenant_id', '', false)")
    conn.commit()


def test_on_borrow_runs_on_every_engine_borrow_before_its_statements(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    assert stabilize("mg-up", "--db-url", db).returncode == 0
    seen: list[str] = []

    def on_borrow(conn: Any) -> None:
        _set_tenant(conn)
        seen.append(TENANT.get())

    store = PostgresWorkflowStore(db, options=PoolOptions(on_borrow=on_borrow, reset=_clear_tenant))
    token = TENANT.set("tenant-a")
    try:
        workflow = Workflow.create(application="hooks", name="borrow", stages=[])
        store.store(workflow)
        assert store.retrieve(workflow.id).id == workflow.id
        with store._pool.connection() as conn:
            row = conn.execute("SELECT current_setting('app.tenant_id', true) AS tenant").fetchone()
        assert row["tenant"] == "tenant-a", f"the engine's statements ran without the hook's setting: {row}"
    finally:
        TENANT.reset(token)
        store.close()
    assert len(seen) >= 3 and set(seen) == {"tenant-a"}, f"on_borrow ran {len(seen)} time(s) with {set(seen)}"


def _leaves_a_transaction(conn: Any) -> None:
    conn.execute("SELECT 1")


def test_on_borrow_that_leaves_a_transaction_open_is_refused_and_the_connection_returned(db: str) -> None:
    manager = get_connection_manager()
    options = PoolOptions(min_size=1, max_size=1, acquire_timeout=5, on_borrow=_leaves_a_transaction)
    pool = manager.get_postgres_pool(db, options=options)
    try:
        for _ in range(2):
            with pytest.raises(BorrowHookError), pool.connection():
                pass
    finally:
        manager.release_postgres_pool(pool)


def test_an_on_borrow_error_reaches_the_caller_and_returns_the_connection(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    assert stabilize("mg-up", "--db-url", db).returncode == 0

    def deny(conn: Any) -> None:
        raise PermissionError("tenant not set")

    store = PostgresWorkflowStore(db, options=PoolOptions(min_size=1, max_size=1, acquire_timeout=5, on_borrow=deny))
    try:
        for _ in range(3):
            with pytest.raises(PermissionError, match="tenant not set"):
                store.retrieve("01JLIVEPROBE0000000000RUN0")
    finally:
        store.close()
