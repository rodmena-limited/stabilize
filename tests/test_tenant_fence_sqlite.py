"""The SQLite fence: triggers installed in tenancy mode, the per-connection tenant function, one tenant per file."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.workflow import Workflow
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.persistence.tenancy import (
    TenancyModeChanged,
    TenantContextError,
    TenantError,
    TenantMismatchError,
    tenant_error_from,
)
from tests.mg_support import output, stabilize

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


@pytest.fixture()
def tenancy_file(tmp_path: Path) -> tuple[str, Path]:
    path = tmp_path / "fence.db"
    url = f"sqlite:///{path}"
    enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
    assert enabled.returncode == 0, output(enabled)
    return url, path


def _triggers(path: Path) -> dict[str, set[str]]:
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute("SELECT tbl_name, name FROM sqlite_master WHERE type = 'trigger'").fetchall()
    finally:
        conn.close()
    found: dict[str, set[str]] = {}
    for table, name in rows:
        found.setdefault(table, set()).add(name)
    return found


def _all_tables(url: str, tenant: str) -> None:
    from stabilize.events.store.sqlite.store import SqliteEventStore
    from stabilize.persistence.sqlite.signals import create_signals_table
    from stabilize.persistence.task_lease import TaskLeaseManager
    from stabilize.queue.sqlite import SqliteQueue

    store = SqliteWorkflowStore(url, create_tables=True, tenant=tenant)
    queue = SqliteQueue(url, tenant=tenant)
    SqliteEventStore(url, tenant=tenant)
    TaskLeaseManager(store)._ensure_table()
    create_signals_table(store._get_connection())
    queue.close()
    store.close()


def test_tables_created_in_tenancy_mode_carry_the_fence_and_disable_removes_it(tenancy_file: tuple[str, Path]) -> None:
    url, path = tenancy_file
    _all_tables(url, "acme")
    triggers = _triggers(path)
    missing = [
        t
        for t in ENGINE_TABLES
        if not {f"stabilize_fence_ins_{t}", f"stabilize_fence_upd_{t}", f"stabilize_fence_del_{t}"}
        <= triggers.get(t, set())
    ]
    assert not missing, f"tables without the three fence triggers: {missing}"

    disabled = stabilize("mg-tenancy", "disable", "--db-url", url)
    assert disabled.returncode == 0, output(disabled)
    assert _triggers(path) == {}, _triggers(path)


def test_a_connection_without_the_function_cannot_write_and_the_fence_refuses_bad_tenants(
    tenancy_file: tuple[str, Path],
) -> None:
    url, path = tenancy_file
    _all_tables(url, "acme")
    insert = (
        "INSERT INTO pipeline_executions (id, type, application, status, stabilize_tenant) "
        "VALUES (?, 'PIPELINE', 'fence', 'RUNNING', ?)"
    )
    shell = sqlite3.connect(path)
    try:
        for tenant in ("", "acme"):
            with pytest.raises(sqlite3.OperationalError, match="no such function: stabilize_tenant") as raised:
                shell.execute(insert, ("01FENCE000000000000000001", tenant))
            assert isinstance(tenant_error_from(raised.value), TenantContextError)
            shell.rollback()
    finally:
        shell.close()

    from stabilize.persistence.connection import get_connection_manager

    conn = get_connection_manager().get_sqlite_connection(url)
    with pytest.raises(TenancyModeChanged, match="ST001"):
        conn.execute(insert, ("01FENCE000000000000000002", ""))
    conn.rollback()
    with pytest.raises(TenantMismatchError, match="ST002"):
        conn.execute(insert, ("01FENCE000000000000000003", "globex"))
    conn.rollback()
    conn.execute(insert, ("01FENCE000000000000000004", "acme"))
    with pytest.raises(TenantMismatchError, match="ST002"):
        conn.execute(
            "UPDATE pipeline_executions SET stabilize_tenant = 'globex' WHERE id = ?", ("01FENCE000000000000000004",)
        )
    conn.rollback()


def test_engine_writes_pass_and_a_second_tenant_on_the_same_file_is_refused(tenancy_file: tuple[str, Path]) -> None:
    url, _ = tenancy_file
    acme = SqliteWorkflowStore(url, create_tables=True, tenant="acme")
    try:
        wf = Workflow.create(application="fenced", name="w", stages=[])
        acme.store(wf)
        assert acme.retrieve(wf.id).id == wf.id
        acme.delete(wf.id)
        with pytest.raises(TenantError, match="globex"):
            SqliteWorkflowStore(url, create_tables=True, tenant="globex")
        again = SqliteWorkflowStore(url, tenant="acme")
        again.close()
    finally:
        acme.close()


def test_memory_files_are_exempt_from_the_one_tenant_rule() -> None:
    first = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True, tenant="")
    second = SqliteWorkflowStore("sqlite:///:memory:", create_tables=True, tenant="")
    first.close()
    second.close()


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (sqlite3.IntegrityError("ST001: stabilize_tenant is empty"), TenancyModeChanged),
        (
            sqlite3.IntegrityError("ST002: row tenant acme does not match the connection tenant globex"),
            TenantMismatchError,
        ),
        (sqlite3.OperationalError("no such function: stabilize_tenant"), TenantContextError),
    ],
)
def test_sqlite_errors_map_to_engine_errors(exc: Exception, expected: type[Any]) -> None:
    assert isinstance(tenant_error_from(exc), expected)


def test_an_unrelated_sqlite_error_is_not_a_tenancy_error() -> None:
    assert tenant_error_from(sqlite3.IntegrityError("UNIQUE constraint failed: task_leases.task_id")) is None
