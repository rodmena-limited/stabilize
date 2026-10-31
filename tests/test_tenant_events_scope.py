"""Per-processor dedup, the event store's tenant stamp, namespaced subscriptions and mg-queue-table (WI-2 slice 6b)."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from stabilize.events.base import EntityType, Event, EventType
from stabilize.persistence.tenancy import TenantError, TenantMismatchError
from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture(scope="module")
def tenancy_db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgevs") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
        assert enabled.returncode == 0, output(enabled)
        yield url


def _event(wf_id: str, tenant: str = "") -> Event:
    return Event(
        event_type=EventType.WORKFLOW_STARTED,
        entity_type=EntityType.WORKFLOW,
        entity_id=wf_id,
        workflow_id=wf_id,
        data={},
        version=1,
        tenant=tenant,
    )


def test_each_processor_owns_its_deduplicator(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.dedup import get_deduplicator
    from stabilize.queue.postgres import PostgresQueue
    from stabilize.queue.processor import QueueProcessor

    parts = [
        (PostgresWorkflowStore(tenancy_db, tenant=t), PostgresQueue(tenancy_db, tenant=t)) for t in ("acme", "globex")
    ]
    try:
        first = QueueProcessor(parts[0][1], store=parts[0][0])
        second = QueueProcessor(parts[1][1], store=parts[1][0])
        assert first.deduplicator is not second.deduplicator
        assert first.deduplicator is not get_deduplicator()
        first.deduplicator.mark_seen("m-1")
        assert first.deduplicator.maybe_seen("m-1") is True
        assert second.deduplicator.maybe_seen("m-1") is False
    finally:
        for store, queue in parts:
            queue.close()
            store.close()


def test_the_event_store_stamps_its_tenant_and_refuses_another(tenancy_db: str) -> None:
    from stabilize.events.store.postgres.store import PostgresEventStore

    acme = PostgresEventStore(tenancy_db, tenant="acme")
    try:
        stored = acme.append(_event("01EVTENANT000000000000001"))
        assert stored.tenant == "acme"
        assert acme.get_event_by_id(stored.event_id).tenant == "acme"
        with pytest.raises(TenantMismatchError, match="globex"):
            acme.append(_event("01EVTENANT000000000000002", tenant="globex"))
        same = acme.append(_event("01EVTENANT000000000000003", tenant="acme"))
        assert same.tenant == "acme"
    finally:
        acme.close()


def test_a_processor_refuses_a_global_recorder_of_another_tenant(tenancy_db: str) -> None:
    from stabilize.events import configure_event_sourcing, reset_event_recorder
    from stabilize.events.store.postgres.store import PostgresEventStore
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue
    from stabilize.queue.processor import QueueProcessor

    events = PostgresEventStore(tenancy_db, tenant="acme")
    acme_store = PostgresWorkflowStore(tenancy_db, tenant="acme")
    acme_queue = PostgresQueue(tenancy_db, tenant="acme")
    globex_store = PostgresWorkflowStore(tenancy_db, tenant="globex")
    globex_queue = PostgresQueue(tenancy_db, tenant="globex")
    try:
        configure_event_sourcing(events, publish_to_bus=False)
        QueueProcessor(acme_queue, store=acme_store)
        with pytest.raises(TenantError, match="globex"):
            QueueProcessor(globex_queue, store=globex_store)
    finally:
        reset_event_recorder()
        for part in (globex_queue, globex_store, acme_queue, acme_store, events):
            part.close()


def test_durable_subscription_names_are_per_tenant(tenancy_db: str) -> None:
    from stabilize.events.store.postgres.store import PostgresEventStore

    acme = PostgresEventStore(tenancy_db, tenant="acme")
    globex = PostgresEventStore(tenancy_db, tenant="globex")
    try:
        acme.save_subscription("projector", event_types=None, entity_filter=None, last_sequence=5)
        globex.save_subscription("projector", event_types=None, entity_filter=None, last_sequence=9)
        assert acme.get_subscription("projector")["last_sequence"] == 5
        assert globex.get_subscription("projector")["last_sequence"] == 9
        assert [s["id"] for s in acme.list_subscriptions()] == ["projector"]
        assert [s["id"] for s in globex.list_subscriptions()] == ["projector"]
        acme.delete_subscription("projector")
        assert acme.get_subscription("projector") is None
        assert globex.get_subscription("projector") is not None
        with pytest.raises(ValueError):
            acme.save_subscription("bad\x1fname", event_types=None, entity_filter=None, last_sequence=0)
        with pytest.raises(ValueError):
            acme.save_subscription("n" * 101, event_types=None, entity_filter=None, last_sequence=0)
    finally:
        globex.close()
        acme.close()


def test_mg_queue_table_creates_a_fenced_custom_queue(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.messages import StartWorkflow
    from stabilize.queue.postgres import PostgresQueue

    created = stabilize("mg-queue-table", "priority_queue", "--db-url", tenancy_db)
    assert created.returncode == 0, output(created)
    again = stabilize("mg-queue-table", "priority_queue", "--db-url", tenancy_db)
    assert again.returncode == 0, output(again)
    refused = stabilize("mg-queue-table", "bad-name;", "--db-url", tenancy_db)
    assert refused.returncode != 0

    with psycopg.connect(tenancy_db) as conn:
        triggers = {
            r[0]
            for r in conn.execute(
                "SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                "WHERE t.tgname = 'stabilize_tenant_fence' AND NOT t.tgisinternal"
            )
        }
        defaults = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT table_name, column_default FROM information_schema.columns "
                "WHERE column_name = 'stabilize_tenant' AND table_name LIKE 'priority_queue%'"
            )
        }
    assert {"priority_queue", "priority_queue_dlq"} <= triggers
    assert defaults == {"priority_queue": None, "priority_queue_dlq": None}

    store = PostgresWorkflowStore(tenancy_db, tenant="acme")
    queue = PostgresQueue(tenancy_db, table_name="priority_queue", tenant="acme")
    try:
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01EVTENANT000000000000009"))
        message = queue.poll_one()
        assert message is not None
        queue.ack(message)
    finally:
        queue.close()
        store.close()
