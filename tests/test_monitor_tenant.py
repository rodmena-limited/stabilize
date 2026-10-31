"""The monitor shows one tenant's rows, or every tenant's on request, and asks for a choice in tenancy mode."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from stabilize.models.workflow import Workflow
from stabilize.monitor.data import MonitorDataFetcher
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.sqlite import SqliteQueue
from tests.mg_support import output, stabilize


@pytest.fixture()
def tenancy_file(tmp_path: Path) -> str:
    url = f"sqlite:///{tmp_path / 'monitor.db'}"
    enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
    assert enabled.returncode == 0, output(enabled)
    return url


def _parts(url: str, tenant: str) -> tuple[Any, Any]:
    from stabilize.persistence.connection import ConnectionManager, SingletonMeta

    SingletonMeta.reset(ConnectionManager)
    store = SqliteWorkflowStore(url, create_tables=True, tenant=tenant)
    return store, SqliteQueue(url, tenant=tenant)


def test_the_monitor_shows_only_the_named_tenant_or_every_tenant_on_request(tenancy_file: str) -> None:
    acme_store, acme_queue = _parts(tenancy_file, "acme")
    globex_store, globex_queue = _parts(tenancy_file, "globex")
    try:
        wf = Workflow.create(application="monitored", name="w", stages=[])
        acme_store.store(wf)
        acme_queue.push(StartWorkflow(execution_type="PIPELINE", execution_id=wf.id))

        seen_by_acme = MonitorDataFetcher(acme_store, acme_queue, tenant="acme").fetch()
        assert seen_by_acme.error is None, seen_by_acme.error
        assert [w.id for w in seen_by_acme.workflows] == [wf.id]
        assert seen_by_acme.queue_stats.pending == 1

        seen_by_globex = MonitorDataFetcher(globex_store, globex_queue, tenant="globex").fetch()
        assert seen_by_globex.error is None, seen_by_globex.error
        assert seen_by_globex.workflows == []
        assert seen_by_globex.queue_stats.pending == 0

        everyone = MonitorDataFetcher(globex_store, globex_queue, tenant=None).fetch()
        assert [w.id for w in everyone.workflows] == [wf.id]
        assert everyone.queue_stats.pending == 1
    finally:
        for part in (acme_queue, acme_store, globex_queue, globex_store):
            part.close()


@pytest.mark.parametrize(
    ("mode", "tenant", "all_tenants", "expected"),
    [
        ("single", None, False, ""),
        ("single", "acme", False, "acme"),
        ("tenancy", "acme", False, "acme"),
        ("tenancy", None, True, None),
        ("single", None, True, None),
    ],
)
def test_the_monitor_resolves_its_tenant_from_the_mode_and_flags(
    mode: str, tenant: str | None, all_tenants: bool, expected: str | None
) -> None:
    from stabilize.cli.commands import resolve_monitor_tenant

    assert resolve_monitor_tenant(mode, tenant, all_tenants) == expected


def test_the_monitor_refuses_tenancy_mode_without_a_choice(capsys: Any) -> None:
    from stabilize.cli.commands import resolve_monitor_tenant

    with pytest.raises(SystemExit) as raised:
        resolve_monitor_tenant("tenancy", None, False)
    assert raised.value.code == 1
    out = capsys.readouterr().out
    assert "--tenant" in out and "--all-tenants" in out
    with pytest.raises(SystemExit):
        resolve_monitor_tenant("single", "acme", True)
