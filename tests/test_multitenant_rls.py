"""One pool serves two tenants under row-level security on every engine table (#101 R6, #97 R5)."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from contextvars import ContextVar
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pytest

from stabilize import StageExecution, TaskResult, Workflow
from stabilize.cli.grants import engine_tables
from stabilize.cli.migrations import get_migrations
from stabilize.events import configure_event_sourcing, reset_event_bus, reset_event_recorder
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.pool_options import PoolOptions
from stabilize.persistence.store import WorkflowNotFoundError
from stabilize.tasks.interface import Task
from tests.audit_support import make_processor, stage, start
from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")

TENANT: ContextVar[str] = ContextVar("stabilize_rls_tenant", default="")


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def _set_tenant(conn: Any) -> None:
    conn.execute("SELECT set_config('app.tenant_id', %s, false)", (TENANT.get(),))
    conn.commit()


def _clear_tenant(conn: Any) -> None:
    conn.execute("SELECT set_config('app.tenant_id', '', false)")
    conn.commit()


OPTIONS = PoolOptions(min_size=1, max_size=8, on_borrow=_set_tenant, reset=_clear_tenant)


@pytest.fixture()
def app_dsn(postgres_container: Any) -> Iterator[tuple[str, str]]:
    with scratch_database(postgres_container, "mgrls") as owner:
        migrated = stabilize("mg-up", "--db-url", owner)
        assert migrated.returncode == 0, output(migrated)
        role, password = f"app_{uuid.uuid4().hex[:8]}", "app-pw-101"
        with psycopg.connect(owner, autocommit=True) as conn:
            conn.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
            conn.execute(f'GRANT USAGE ON SCHEMA public TO "{role}"')
            for table in engine_tables(get_migrations()):
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS tenant_id text "
                    "NOT NULL DEFAULT current_setting('app.tenant_id', true)"
                )
                conn.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
                conn.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
                conn.execute(f"CREATE POLICY tenant_access ON {table} FOR ALL USING (true) WITH CHECK (true)")
                conn.execute(
                    f"CREATE POLICY tenant_isolation ON {table} AS RESTRICTIVE "
                    "USING (tenant_id = current_setting('app.tenant_id', true)) "
                    "WITH CHECK (tenant_id = current_setting('app.tenant_id', true))"
                )
                conn.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO "{role}"')
            conn.execute(f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "{role}"')
        parts = urlsplit(owner)
        app = urlunsplit((parts.scheme, f"{role}:{password}@{parts.hostname}:{parts.port}", parts.path, "", ""))
        try:
            yield owner, app
        finally:
            with psycopg.connect(owner, autocommit=True) as conn:
                conn.execute(f'DROP OWNED BY "{role}"')
                conn.execute(f'DROP ROLE IF EXISTS "{role}"')


def _finished(store: Any, workflow_id: str, seconds: float = 60.0) -> Workflow:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        workflow = store.retrieve(workflow_id)
        if workflow.status.is_complete:
            return workflow
        time.sleep(0.1)
    raise AssertionError(f"workflow {workflow_id} did not finish in {seconds} s")


def test_one_pool_serves_two_tenants_under_rls_on_every_engine_table(app_dsn: tuple[str, str]) -> None:
    from stabilize.events.store.postgres import PostgresEventStore
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue import PostgresQueue

    owner, app = app_dsn
    tenants = ("tenant-a", "tenant-b")
    stores, queues, processors, workflows = {}, {}, {}, {}
    events = PostgresEventStore(app, options=OPTIONS)
    configure_event_sourcing(events)
    try:
        for tenant in tenants:
            token = TENANT.set(tenant)
            try:
                stores[tenant] = PostgresWorkflowStore(app, options=OPTIONS)
                queues[tenant] = PostgresQueue(app, options=OPTIONS)
                processors[tenant] = make_processor(stores[tenant], queues[tenant], {"ok": Ok})
                processors[tenant].start()
                stages = [stage("a", ["ok"]), stage("b", ["ok"], needs={"a"})]
                workflows[tenant] = start(stores[tenant], queues[tenant], stages)
            finally:
                TENANT.reset(token)
        pools = {id(stores[t]._pool) for t in tenants} | {id(queues[t]._pool) for t in tenants} | {id(events._pool)}
        assert len(pools) == 1, f"the tenants' stores, queues and the event store use {len(pools)} pools, expected one"

        for tenant in tenants:
            token = TENANT.set(tenant)
            try:
                final = _finished(stores[tenant], workflows[tenant].id)
                assert final.status == WorkflowStatus.SUCCEEDED, f"{tenant}'s workflow ended {final.status.name}"
                other = workflows["tenant-b" if tenant == "tenant-a" else "tenant-a"].id
                with pytest.raises(WorkflowNotFoundError):
                    stores[tenant].retrieve(other)
                with stores[tenant]._pool.connection() as conn:
                    visible = conn.execute("SELECT count(*) AS n FROM pipeline_executions").fetchone()["n"]
                assert visible == 1, f"{tenant} sees {visible} workflows, expected only its own"
                assert events.get_events_for_workflow(workflows[tenant].id), f"{tenant} reads none of its own events"
                assert events.get_events_for_workflow(other) == [], f"{tenant} reads the other tenant's events"
            finally:
                TENANT.reset(token)
    finally:
        for processor in processors.values():
            processor.stop(wait=True)
        reset_event_bus()
        reset_event_recorder()
        events.close()
        for queue in queues.values():
            queue.close()
        for store in stores.values():
            store.close()

    with psycopg.connect(owner, autocommit=True) as conn:
        for table in ("pipeline_executions", "stage_executions", "events"):
            conn.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
        total = conn.execute("SELECT count(*) FROM pipeline_executions").fetchone()[0]
        tagged = conn.execute("SELECT count(DISTINCT tenant_id) FROM stage_executions").fetchone()[0]
        event_tenants = conn.execute("SELECT array_agg(DISTINCT tenant_id ORDER BY tenant_id) FROM events")
        event_tenants = event_tenants.fetchone()[0]
    assert total == 2, f"the bypass control sees {total} workflows, expected both tenants' 2"
    assert tagged == 2, f"stage rows carry {tagged} tenant ids, expected one per tenant"
    assert event_tenants == list(tenants), f"events were written under {event_tenants}, expected {list(tenants)}"
