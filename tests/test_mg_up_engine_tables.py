"""The event store and task-lease tables come from stabilize mg-up, not from runtime DDL (#95)."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pytest

from tests.mg_support import (
    old_runner,
    old_stabilize,
    output,
    relation_exists,
    scratch_database,
    search_path_url,
    stabilize,
    with_schema,
)

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgeng") as url:
        yield url


def _mg_up(url: str) -> None:
    result = stabilize("mg-up", "--db-url", url)
    assert result.returncode == 0, output(result)


def _as_role(url: str, role: str, password: str) -> str:
    parts = urlsplit(url)
    host = parts.netloc.rpartition("@")[2]
    return urlunsplit((parts.scheme, f"{role}:{password}@{host}", parts.path, parts.query, ""))


@pytest.fixture()
def dml_only_role(db: str) -> Iterator[str]:
    role, password = f"rt_{uuid.uuid4().hex[:10]}", "rt-pw-95"
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        conn.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    try:
        yield _as_role(db, role, password)
    finally:
        with psycopg.connect(db, autocommit=True) as conn:
            conn.execute(f'DROP OWNED BY "{role}"')
            conn.execute(f'DROP ROLE IF EXISTS "{role}"')


def _grant_dml(db: str, runtime_url: str) -> None:
    role = urlsplit(runtime_url).username
    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute(f'GRANT USAGE ON SCHEMA public TO "{role}"')
        conn.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{role}"')
        conn.execute(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{role}"')


def test_the_event_store_constructs_on_a_database_set_up_by_mg_up(db: str) -> None:
    from stabilize.events.store.postgres.store import PostgresEventStore

    _mg_up(db)
    try:
        store = PostgresEventStore(db)
    except Exception as exc:
        pytest.fail(f"the event store refused a database set up by mg-up: {type(exc).__name__}: {exc}")
    try:
        assert store.supports_commit_cursor(), "mg-up did not add events.commit_xid"
    finally:
        store.close()


def test_the_event_store_tables_follow_the_target_schema(db: str) -> None:
    from stabilize.events.store.postgres.store import PostgresEventStore

    _mg_up(with_schema(db, "ev"))
    try:
        store = PostgresEventStore(search_path_url(db, "ev"), schema="ev")
    except Exception as exc:
        pytest.fail(f"the event store refused a database set up by mg-up: {type(exc).__name__}: {exc}")
    store.close()
    assert not relation_exists(db, "public.events"), "event store tables landed in public"


def test_the_event_store_refusal_names_mg_up(db: str) -> None:
    from stabilize.events.store.postgres.store import PostgresEventStore

    for _attempt in range(5):
        with pytest.raises(Exception) as raised:
            PostgresEventStore(db)
        assert "stabilize mg-up" in str(raised.value), (
            f"the event store's refusal does not name stabilize mg-up: {raised.value}"
        )
    assert not relation_exists(db, "events"), "the event store created its own tables"


def test_a_dml_only_runtime_role_can_lease_tasks_after_mg_up(db: str, dml_only_role: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.persistence.task_lease import TaskLeaseManager

    _mg_up(db)
    _grant_dml(db, dml_only_role)
    store = PostgresWorkflowStore(connection_string=dml_only_role)
    try:
        try:
            first = TaskLeaseManager(store, owner="worker-a")
            second = TaskLeaseManager(store, owner="worker-b")
        except Exception as exc:
            pytest.fail(f"task leasing could not start for a DML-only role after mg-up: {type(exc).__name__}: {exc}")
        assert first.acquire("task-95") is True
        assert second.acquire("task-95") is False, "a second owner took a lease that was still held"
    finally:
        store.close()


def test_task_leasing_on_an_old_database_names_mg_up_and_creates_nothing(db: str, tmp_path: Path) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.persistence.task_lease import TaskLeaseManager

    built = old_stabilize(old_runner("0.33.1"), "mg-up", "--db-url", db, cwd=tmp_path)
    assert built.returncode == 0, output(built)
    store = PostgresWorkflowStore(connection_string=db)
    refusal = ""
    try:
        TaskLeaseManager(store, owner="worker-a")
    except Exception as exc:
        refusal = str(exc)
    finally:
        store.close()
    assert not relation_exists(db, "task_leases"), "task leasing created its own table at runtime"
    assert "stabilize mg-up" in refusal, f"task leasing did not name stabilize mg-up: {refusal!r}"


def test_an_unmigrated_database_names_mg_up_from_the_first_statement(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.persistence.task_lease import TaskLeaseManager
    from stabilize.persistence.tenancy import TenancyModeError

    store = PostgresWorkflowStore(connection_string=db)
    try:
        with pytest.raises(TenancyModeError, match="stabilize mg-up") as refusal:
            TaskLeaseManager(store, owner="worker-a")
    finally:
        store.close()
    assert "stabilize_tenancy" in str(refusal.value)
    assert not relation_exists(db, "stabilize_tenancy"), "the engine created the mode table at runtime"
