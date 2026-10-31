"""WI-9 slice 9e (A12, B38): the monitor counts its queue in UTC through the queue API and lists through the store."""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.monitor.data import MonitorDataFetcher
from stabilize.queue.messages import StartWorkflow
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgmon") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'monitor.db'}"
        return
    yield request.getfixturevalue("db")


def _parts(url: str) -> tuple[Any, Any]:
    if url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url)
        queue._create_table()
        return store, queue
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    return PostgresWorkflowStore(url), PostgresQueue(url)


@pytest.fixture()
def tokyo(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    time.tzset()
    try:
        yield
    finally:
        monkeypatch.undo()
        time.tzset()


def _counts(store: Any, queue: Any) -> tuple[int, int, int]:
    data = MonitorDataFetcher(store, queue, tenant="").fetch()
    assert data.error is None, data.error
    return data.queue_stats.pending, data.queue_stats.processing, data.queue_stats.stuck


def test_queue_counts_hold_in_a_non_utc_zone(any_db: str, tokyo: None) -> None:
    store, queue = _parts(any_db)
    try:
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01M68S00000000000000000001"))
        queue.push(
            StartWorkflow(execution_type="PIPELINE", execution_id="01M68S00000000000000000002"),
            delay=timedelta(hours=2),
        )
        assert _counts(store, queue) == (1, 0, 0), "a message due in two hours was counted, or the due one was not"
        leased = queue.poll_one()
        assert leased is not None
        assert _counts(store, queue) == (0, 1, 0), "a message leased a moment ago was not counted as processing"
    finally:
        queue.clear()
        queue.close()
        store.close()


def test_queue_counts_follow_the_queue_table(tmp_path: Path) -> None:
    from stabilize.persistence.sqlite import SqliteWorkflowStore
    from stabilize.queue.sqlite import SqliteQueue

    url = f"sqlite:///{tmp_path / 'custom.db'}"
    store = SqliteWorkflowStore(url, create_tables=True)
    custom = SqliteQueue(url, table_name="custom_queue")
    custom._create_table()
    try:
        custom.push(StartWorkflow(execution_type="PIPELINE", execution_id="01M68S00000000000000000003"))
        assert _counts(store, custom) == (1, 0, 0), (
            "the monitor counted queue_messages instead of the queue's own table"
        )
    finally:
        custom.clear()
        custom.close()
        store.close()


def _running_workflow(store: Any, application: str, name: str, status: WorkflowStatus) -> Workflow:
    task = TaskExecution.create(name="t", implementing_class="noop", stage_start=True, stage_end=True)
    stage = StageExecution(ref_id="s0", type="noop", name="S0", tasks=[task])
    wf = Workflow.create(application=application, name=name, stages=[stage])
    now = int(time.time() * 1000)
    wf.status = status
    wf.start_time = now
    stage.status = status if status is not WorkflowStatus.RUNNING else WorkflowStatus.RUNNING
    stage.start_time = now
    task.status = stage.status
    task.start_time = now
    if status is WorkflowStatus.SUCCEEDED:
        wf.end_time = stage.end_time = task.end_time = now
    store.store(wf)
    return wf


def test_a_single_tenant_view_reads_through_the_store_api(any_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    store, queue = _parts(any_db)
    try:
        running = _running_workflow(store, "shown", "running", WorkflowStatus.RUNNING)
        done = _running_workflow(store, "shown", "done", WorkflowStatus.SUCCEEDED)

        def raw_sql_used(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("the single-tenant monitor built its own SQL instead of using the store API")

        monkeypatch.setattr(MonitorDataFetcher, "_build_workflow_query", raw_sql_used)
        fetcher = MonitorDataFetcher(store, queue, tenant="")
        data = fetcher.fetch(app_filter="shown")
        assert data.error is None, data.error
        assert {w.id for w in data.workflows} == {running.id, done.id}
        shown = next(w for w in data.workflows if w.id == running.id)
        assert [s.ref_id for s in shown.stages] == ["s0"]
        assert [t.name for t in shown.stages[0].tasks] == ["t"]
        assert data.workflows[0].id == running.id, "running workflows come first"
        only_running = fetcher.fetch(app_filter="shown", status_filter="running")
        assert [w.id for w in only_running.workflows] == [running.id]
        one = fetcher.fetch(app_filter="shown", limit=1)
        assert len(one.workflows) == 1
        everything = fetcher.fetch()
        assert {w.id for w in everything.workflows} >= {running.id, done.id}
    finally:
        queue.close()
        store.close()
