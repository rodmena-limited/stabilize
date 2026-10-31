"""Two tenants on one schema or file in tenancy mode: no public operation of one sees the other's rows."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

from stabilize.events.base import EntityType, Event, EventType
from stabilize.events.store.interface import EventQuery
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowNotFoundError
from stabilize.persistence.tenancy import TenantMismatchError
from stabilize.queue.messages import StartWorkflow
from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture(scope="module", params=["postgres", "sqlite"])
def tenancy_db(request: Any, tmp_path_factory: Any) -> Iterator[str]:
    if request.param == "sqlite":
        url = f"sqlite:///{tmp_path_factory.mktemp('iso') / 'tenancy.db'}"
        enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
        assert enabled.returncode == 0, output(enabled)
        yield url
        return
    postgres_container = request.getfixturevalue("postgres_container")
    with scratch_database(postgres_container, "mgiso") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
        assert enabled.returncode == 0, output(enabled)
        yield url


@dataclass
class Parts:
    tenant: str
    store: Any
    queue: Any
    events: Any

    def close(self) -> None:
        if hasattr(self.events, "close"):
            self.events.close()
        self.queue.close()
        self.store.close()


def _build(url: str, tenant: str) -> Parts:
    if url.startswith("sqlite:"):
        from stabilize.events.store.sqlite.store import SqliteEventStore
        from stabilize.persistence.connection import ConnectionManager, SingletonMeta
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        SingletonMeta.reset(ConnectionManager)
        store = SqliteWorkflowStore(url, create_tables=True, tenant=tenant)
        return Parts(tenant, store, SqliteQueue(url, tenant=tenant), SqliteEventStore(url, tenant=tenant))
    from stabilize.events.store.postgres.store import PostgresEventStore
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    return Parts(
        tenant,
        PostgresWorkflowStore(url, tenant=tenant),
        PostgresQueue(url, tenant=tenant),
        PostgresEventStore(url, tenant=tenant),
    )


@pytest.fixture()
def tenants(tenancy_db: str) -> Iterator[tuple[Parts, Parts]]:
    built = [_build(tenancy_db, tenant) for tenant in ("acme", "globex")]
    try:
        yield built[0], built[1]
    finally:
        for parts in built:
            parts.close()


def _workflow(application: str = "iso") -> Workflow:
    return Workflow.create(
        application=application,
        name="isolation",
        stages=[
            StageExecution(
                ref_id="s1",
                name="first",
                tasks=[TaskExecution.create(name="t", implementing_class="stabilize.tasks.ShellTask")],
            ),
            StageExecution(ref_id="s2", name="second", requisite_stage_ref_ids={"s1"}, tasks=[]),
        ],
    )


def test_workflows_stages_and_tasks_are_invisible_to_the_other_tenant(tenants: tuple[Parts, Parts]) -> None:
    a, b = tenants
    wf = _workflow()
    a.store.store(wf)
    stage = wf.stages[0]

    assert a.store.retrieve(wf.id).id == wf.id
    assert a.store.retrieve_stage(stage.id).id == stage.id
    assert [s.ref_id for s in a.store.get_downstream_stages(wf.id, "s1")] == ["s2"]

    with pytest.raises(WorkflowNotFoundError):
        b.store.retrieve(wf.id)
    with pytest.raises(WorkflowNotFoundError):
        b.store.retrieve_execution_summary(wf.id)
    with pytest.raises((ValueError, WorkflowNotFoundError)):
        b.store.retrieve_stage(stage.id)
    assert b.store.get_upstream_stages(wf.id, "s2") == []
    assert b.store.get_downstream_stages(wf.id, "s1") == []
    assert b.store.get_synthetic_stages(wf.id, stage.id) == []
    assert b.store.get_merged_ancestor_outputs(wf.id, "s2") == {}
    assert list(b.store.retrieve_by_application("iso")) == []
    assert list(b.store.retrieve_by_pipeline_config_id(wf.pipeline_config_id or "none")) == []
    assert b.store.count_by_application("iso") == 0
    assert list(b.store.get_all_pending_workflows()) == []
    assert b.store.exists(wf.id) is False
    assert a.store.exists(wf.id) is True


def test_writes_from_the_other_tenant_do_not_touch_the_rows(tenants: tuple[Parts, Parts]) -> None:
    a, b = tenants
    wf = _workflow("writes")
    a.store.store(wf)
    before = a.store.retrieve(wf.id)

    for attempt in (
        lambda: b.store.pause(wf.id, "intruder"),
        lambda: b.store.cancel(wf.id, "intruder", "because"),
        lambda: b.store.delete(wf.id),
    ):
        try:
            attempt()
        except WorkflowNotFoundError:
            pass

    foreign = a.store.retrieve(wf.id)
    foreign.status = WorkflowStatus.TERMINAL
    try:
        with b.store.transaction() as txn:
            txn.update_workflow_status(foreign)
    except Exception:
        pass

    after = a.store.retrieve(wf.id)
    assert after.paused is None
    assert after.is_canceled is False
    assert after.status == before.status
    assert len(after.stages) == len(before.stages)


def test_queue_and_dead_letters_are_per_tenant(tenants: tuple[Parts, Parts]) -> None:
    a, b = tenants
    wf = _workflow("queue")
    a.store.store(wf)
    a.queue.push(StartWorkflow(execution_type="PIPELINE", execution_id=wf.id))

    assert b.queue.size() == 0
    assert b.queue.poll_one() is None
    assert b.queue.has_pending_message_for_execution(wf.id) is False
    assert a.queue.has_pending_message_for_execution(wf.id) is True
    assert b.queue.purge_workflow(wf.id) == 0
    assert a.queue.has_pending_message_for_execution(wf.id) is True

    message = a.queue.poll_one()
    assert message is not None and message.execution_id == wf.id
    a.queue.move_to_dlq(message.message_id, "parked for the test")
    assert a.queue.dlq_size() == 1
    assert b.queue.dlq_size() == 0
    assert b.queue.list_dlq() == []
    dlq_id = a.queue.list_dlq()[0]["id"]
    assert b.queue.replay_dlq(dlq_id) is False
    assert b.queue.clear_dlq() == 0
    assert a.queue.dlq_size() == 1
    assert b.queue.check_and_move_expired() == 0
    assert a.queue.clear_dlq() == 1


def test_signals_processed_marks_claims_and_leases_are_per_tenant(tenants: tuple[Parts, Parts]) -> None:
    from stabilize.persistence.task_lease import TaskLeaseManager

    a, b = tenants
    wf = _workflow("signals")
    a.store.store(wf)

    assert a.store.buffer_signal(wf.id, "s1", "go", {"k": 1}) > 0
    assert a.store.pending_signal_count(wf.id, "s1") == 1
    assert b.store.pending_signal_count(wf.id, "s1") == 0
    assert b.store.consume_signal(wf.id, "s1") is None
    assert b.store.discard_signals(wf.id, "s1") == 0
    assert a.store.pending_signal_count(wf.id, "s1") == 1

    a.store.mark_message_processed("m-iso-1", "StartWorkflow", wf.id)
    assert a.store.is_message_processed("m-iso-1") is True
    assert b.store.is_message_processed("m-iso-1") is False
    assert "m-iso-1" not in (b.store.get_processed_message_ids() or [])

    with a.store.transaction() as txn:
        assert txn.acquire_claim(wf.id, "choice:g", wf.stages[0].id) is True
    with pytest.raises(TenantMismatchError), b.store.transaction() as txn:
        txn.acquire_claim(wf.id, "choice:g", wf.stages[0].id)
    with b.store.transaction() as txn:
        assert txn.acquire_claim(_workflow().id, "choice:g", wf.stages[0].id) is True

    assert TaskLeaseManager(a.store).acquire("lease-iso") is True
    with pytest.raises(TenantMismatchError):
        TaskLeaseManager(b.store).acquire("lease-iso")
    assert TaskLeaseManager(b.store).acquire("lease-iso-b") is True


def test_events_and_snapshots_are_per_tenant(tenants: tuple[Parts, Parts]) -> None:
    a, b = tenants
    wf = _workflow("events")
    event = Event(
        event_type=EventType.WORKFLOW_STARTED,
        entity_type=EntityType.WORKFLOW,
        entity_id=wf.id,
        workflow_id=wf.id,
        data={"tenant": "acme"},
        version=1,
    )
    stored = a.events.append(event)
    assert stored.sequence is not None

    assert list(b.events.get_events(EventQuery(workflow_id=wf.id))) == []
    assert list(b.events.get_events_for_workflow(wf.id)) == []
    assert list(b.events.get_events_for_entity(EntityType.WORKFLOW, wf.id)) == []
    assert list(b.events.get_events_since(0)) == []
    assert b.events.get_event_by_id(stored.event_id) is None
    assert len(list(a.events.get_events_for_workflow(wf.id))) == 1

    a.events.save_snapshot(EntityType.WORKFLOW, wf.id, wf.id, 1, stored.sequence, {"status": "RUNNING"})
    assert a.events.get_latest_snapshot(EntityType.WORKFLOW, wf.id) is not None
    assert b.events.get_latest_snapshot(EntityType.WORKFLOW, wf.id) is None


def test_stage_queries_are_per_tenant(tenants: tuple[Parts, Parts]) -> None:
    from stabilize.persistence.store import StageCriteria

    a, b = tenants
    wf = _workflow("stages")
    a.store.store(wf)
    mine = StageCriteria(execution_id=wf.id)
    assert {s.id for s in a.store.retrieve_stages(mine)} == {s.id for s in wf.stages}
    assert a.store.count_stages(mine) == len(wf.stages)
    assert b.store.retrieve_stages(mine) == []
    assert b.store.count_stages(mine) == 0
    assert b.store.count_stages(StageCriteria(application=wf.application)) == 0
