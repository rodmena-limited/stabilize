"""First-use schema check, tenant indexes, tenant-scoped admission locks, Workflow.tenant and current_tenant()."""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.tenancy import EngineSchemaError
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")

TENANT_INDEXES = {
    "idx_queue_tenant_poll",
    "idx_queue_tenant_attempts",
    "idx_executions_tenant_config_status",
    "idx_executions_tenant_status",
    "idx_stages_tenant_status_id",
    "idx_events_tenant_sequence",
    "idx_events_tenant_commit",
    "idx_queue_pipeline_config_id",
}


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgschk") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture()
def tenancy_db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgschk2") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
        assert enabled.returncode == 0, output(enabled)
        yield url


def test_mg_up_creates_the_tenant_indexes(db: str) -> None:
    with psycopg.connect(db) as conn:
        names = {r[0] for r in conn.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")}
    assert TENANT_INDEXES <= names, sorted(TENANT_INDEXES - names)


def test_a_missing_default_in_single_mode_is_refused_with_the_ddl(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("ALTER TABLE stage_executions ALTER COLUMN stabilize_tenant DROP DEFAULT")
    store = PostgresWorkflowStore(db)
    try:
        with pytest.raises(
            EngineSchemaError, match="ALTER TABLE stage_executions ALTER COLUMN stabilize_tenant SET DEFAULT ''"
        ):
            store.count_by_application("nobody")
        with pytest.raises(EngineSchemaError):
            store.count_by_application("nobody")
    finally:
        store.close()


def test_a_missing_tenant_column_on_the_queue_table_is_refused_with_the_ddl(db: str) -> None:
    from stabilize.queue.postgres import PostgresQueue

    with psycopg.connect(db, autocommit=True) as conn:
        conn.execute("ALTER TABLE queue_messages_dlq DROP COLUMN stabilize_tenant")
    queue = PostgresQueue(db)
    try:
        with pytest.raises(
            EngineSchemaError,
            match="ALTER TABLE queue_messages_dlq ADD COLUMN stabilize_tenant TEXT NOT NULL DEFAULT ''",
        ):
            queue.dlq_size()
    finally:
        queue.close()


def test_a_missing_fence_in_tenancy_mode_is_refused(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    healthy = PostgresWorkflowStore(tenancy_db, tenant="acme")
    try:
        assert healthy.count_by_application("nobody") == 0
    finally:
        healthy.close()
    with psycopg.connect(tenancy_db, autocommit=True) as conn:
        conn.execute("DROP TRIGGER stabilize_tenant_fence ON task_executions")
    store = PostgresWorkflowStore(tenancy_db, tenant="acme")
    try:
        with pytest.raises(EngineSchemaError, match="stabilize_tenant_fence.*task_executions"):
            store.count_by_application("nobody")
    finally:
        store.close()


def test_a_sqlite_file_missing_its_fence_is_repaired_at_first_use(tmp_path: Path) -> None:
    from stabilize.persistence.sqlite import SqliteWorkflowStore

    path = tmp_path / "check.db"
    url = f"sqlite:///{path}"
    assert stabilize("mg-tenancy", "enable", "--db-url", url).returncode == 0
    SqliteWorkflowStore(url, create_tables=True, tenant="acme").close()
    shell = sqlite3.connect(path)
    shell.execute("DROP TRIGGER stabilize_fence_del_task_executions")
    shell.commit()
    shell.close()
    from stabilize.persistence.connection import ConnectionManager, SingletonMeta

    SingletonMeta.reset(ConnectionManager)
    SqliteWorkflowStore(url, tenant="acme").close()
    shell = sqlite3.connect(path)
    try:
        names = {r[0] for r in shell.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'")}
    finally:
        shell.close()
    assert "stabilize_fence_del_task_executions" in names, "the engine did not put the fence back"


def test_tenants_do_not_block_each_other_on_the_admission_lock(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    acme = PostgresWorkflowStore(tenancy_db, tenant="acme")
    globex = PostgresWorkflowStore(tenancy_db, tenant="globex")
    acme_again = PostgresWorkflowStore(tenancy_db, tenant="acme")
    finished: dict[str, float] = {}

    def take(store: Any, label: str) -> None:
        started = time.monotonic()
        with store.transaction() as txn:
            txn.lock_admission("cfg-1")
            finished[label] = time.monotonic() - started

    try:
        with acme.transaction() as txn:
            txn.lock_admission("cfg-1")
            other = threading.Thread(target=take, args=(globex, "globex"))
            other.start()
            other.join(timeout=5)
            assert "globex" in finished, "another tenant waited on acme's admission lock"
            same = threading.Thread(target=take, args=(acme_again, "acme"))
            same.start()
            same.join(timeout=1.0)
            assert "acme" not in finished, "the same tenant did not wait on its own admission lock"
        same.join(timeout=10)
        assert "acme" in finished
    finally:
        acme_again.close()
        globex.close()
        acme.close()


def test_workflow_carries_its_tenant_and_tenant_ids_must_be_ulids(tenancy_db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    store = PostgresWorkflowStore(tenancy_db, tenant="acme")
    try:
        wf = Workflow.create(application="ids", name="w", stages=[])
        store.store(wf)
        loaded = store.retrieve(wf.id)
        assert loaded.tenant == "acme"
        assert wf.tenant == "" and store.retrieve(wf.id).stages == []

        custom = Workflow.create(application="ids", name="w", stages=[])
        custom.id = "custom-id-1"
        with pytest.raises(ValueError, match="custom-id-1"):
            store.store(custom)
    finally:
        store.close()


class _Records(Task):
    def execute(self, stage: Any) -> TaskResult:
        import stabilize

        return TaskResult.success(outputs={"seen_tenant": stabilize.current_tenant()})


def test_current_tenant_is_bound_while_a_task_runs(tenancy_db: str) -> None:
    import stabilize
    from stabilize.orchestrator import Orchestrator
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue
    from stabilize.queue.processor import QueueProcessor
    from stabilize.tasks.registry import TaskRegistry

    assert stabilize.current_tenant() == ""
    store = PostgresWorkflowStore(tenancy_db, tenant="acme")
    queue = PostgresQueue(tenancy_db, tenant="acme")
    registry = TaskRegistry()
    registry.register("records_tenant", _Records)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    try:
        wf = Workflow.create(
            application="ctx",
            name="w",
            stages=[
                StageExecution(
                    ref_id="s0",
                    type="records_tenant",
                    name="S0",
                    tasks=[
                        TaskExecution.create(
                            name="t", implementing_class="records_tenant", stage_start=True, stage_end=True
                        )
                    ],
                )
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        processor.process_all(timeout=30)
        done = store.retrieve(wf.id)
        assert done.stages[0].outputs.get("seen_tenant") == "acme", done.stages[0].outputs
        assert stabilize.current_tenant() == ""
    finally:
        queue.close()
        store.close()
