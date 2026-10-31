"""Tenant flags on the maintenance commands, factories, the orchestrator singleton and setup_ddl (WI-2 slice 6c)."""

from __future__ import annotations

import time
import warnings
from collections.abc import Iterator
from typing import Any

import pytest

from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import Workflow
from stabilize.persistence.tenancy import TenantError
from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def tenancy_db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgcli") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
        assert enabled.returncode == 0, output(enabled)
        yield url


def _parts(url: str, tenant: str) -> tuple[Any, Any]:
    from stabilize.persistence.factory import create_queue, create_repository

    return create_repository(url, create_tables=False, tenant=tenant), create_queue(url, tenant=tenant)


def test_prune_processed_is_scoped_by_tenant(tenancy_db: str) -> None:
    acme, acme_q = _parts(tenancy_db, "acme")
    globex, globex_q = _parts(tenancy_db, "globex")
    try:
        acme.mark_message_processed("1", "StartWorkflow", "01CLI000000000000000000001")
        globex.mark_message_processed("2", "StartWorkflow", "01CLI000000000000000000002")
        time.sleep(0.05)

        refused = stabilize("prune-processed", "--db-url", tenancy_db, "--older-than-hours", "0")
        assert refused.returncode == 1, output(refused)
        assert "--tenant" in output(refused) and "--all-tenants" in output(refused)
        assert acme.is_message_processed("1") and globex.is_message_processed("2")

        pruned = stabilize("prune-processed", "--db-url", tenancy_db, "--older-than-hours", "0", "--tenant", "acme")
        assert pruned.returncode == 0, output(pruned)
        assert not acme.is_message_processed("1")
        assert globex.is_message_processed("2"), "another tenant's record was pruned"

        everyone = stabilize("prune-processed", "--db-url", tenancy_db, "--older-than-hours", "0", "--all-tenants")
        assert everyone.returncode == 0, output(everyone)
        assert not globex.is_message_processed("2")
    finally:
        for part in (acme_q, acme, globex_q, globex):
            part.close()


def _finished(store: Any, application: str) -> Workflow:
    wf = Workflow.create(application=application, name="done", stages=[])
    wf.status = WorkflowStatus.SUCCEEDED
    wf.start_time = int(time.time() * 1000) - 10_000
    wf.end_time = int(time.time() * 1000) - 5_000
    store.store(wf)
    return wf


def test_purge_workflows_is_scoped_by_tenant(tenancy_db: str) -> None:
    acme, acme_q = _parts(tenancy_db, "acme")
    globex, globex_q = _parts(tenancy_db, "globex")
    try:
        a = _finished(acme, "purge")
        g = _finished(globex, "purge")
        refused = stabilize("purge-workflows", "--db-url", tenancy_db, "--completed-before-days", "0")
        assert refused.returncode == 1, output(refused)
        purged = stabilize(
            "purge-workflows", "--db-url", tenancy_db, "--completed-before-days", "0", "--tenant", "acme"
        )
        assert purged.returncode == 0, output(purged)
        assert acme.exists(a.id) is False
        assert globex.exists(g.id) is True, "another tenant's workflow was purged"
    finally:
        for part in (acme_q, acme, globex_q, globex):
            part.close()


def test_prune_signals_needs_a_tenant_choice_in_tenancy_mode(tenancy_db: str) -> None:
    refused = stabilize("prune-signals", "--db-url", tenancy_db, "--dry-run")
    assert refused.returncode == 1, output(refused)
    assert "--tenant" in output(refused)
    allowed = stabilize("prune-signals", "--db-url", tenancy_db, "--dry-run", "--tenant", "acme")
    assert allowed.returncode == 0, output(allowed)


def test_the_orchestrator_singleton_is_refused_under_a_tenant(tenancy_db: str, postgres_url: str) -> None:
    from stabilize.orchestrator import Orchestrator

    acme, acme_q = _parts(tenancy_db, "acme")
    try:
        Orchestrator(acme_q, store=acme)
        with pytest.raises(TenantError):
            Orchestrator.get_instance()
    finally:
        acme_q.close()
        acme.close()
    plain, plain_q = _parts(postgres_url, "")
    try:
        single = Orchestrator(plain_q, store=plain)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert Orchestrator.get_instance() is single
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)
    finally:
        plain_q.close()
        plain.close()


def test_setup_ddl_matches_the_mg_up_catalog(postgres_container: Any) -> None:
    from stabilize.events.store.postgres.schema import setup_ddl

    def shape(url: str) -> dict[str, list[tuple[str, str, str]]]:
        with psycopg.connect(url) as conn:
            rows = conn.execute(
                "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name IN ('events', 'snapshots', 'event_subscriptions') "
                "ORDER BY table_name, column_name"
            ).fetchall()
        out: dict[str, list[tuple[str, str, str]]] = {}
        for table, column, data_type, nullable in rows:
            out.setdefault(table, []).append((column, data_type, nullable))
        return out

    with (
        scratch_database(postgres_container, "mgddl1") as migrated,
        scratch_database(postgres_container, "mgddl2") as handmade,
    ):
        assert stabilize("mg-up", "--db-url", migrated).returncode == 0
        with psycopg.connect(handmade, autocommit=True) as conn:
            conn.execute(setup_ddl())
        assert shape(handmade) == shape(migrated)
