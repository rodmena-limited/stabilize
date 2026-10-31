"""WI-9 slice 9a: stages load in id order, listings order and page deterministically, windows exclude the unstarted."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mghyg") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'hygiene.db'}"
        return
    yield request.getfixturevalue("db")


def _store(url: str) -> Any:
    if url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore

        return SqliteWorkflowStore(url, create_tables=True)
    from stabilize.persistence.postgres import PostgresWorkflowStore

    return PostgresWorkflowStore(url)


def _task() -> TaskExecution:
    return TaskExecution.create(name="t", implementing_class="noop", stage_start=True, stage_end=True)


def _stage(stage_id: str, ref_id: str, **extra: Any) -> StageExecution:
    return StageExecution(id=stage_id, ref_id=ref_id, type="noop", name=ref_id.upper(), tasks=[_task()], **extra)


HIGH = "01ZZZZZZZZZZZZZZZZZZZZZZZZ"
LOW = "01AAAAAAAAAAAAAAAAAAAAAAAA"
MID = "01MMMMMMMMMMMMMMMMMMMMMMMM"


def test_stages_load_in_id_order_whatever_the_insertion_order(any_db: str) -> None:
    store = _store(any_db)
    try:
        later = _stage(HIGH, "b")
        earlier = _stage(LOW, "a")
        joiner = _stage(MID, "c", requisite_stage_ref_ids={"a", "b"})
        earlier.status = WorkflowStatus.RUNNING
        earlier.start_time = 1
        wf = Workflow.create(application="order", name="w", stages=[later, joiner, earlier])
        wf.status = WorkflowStatus.RUNNING
        store.store(wf)
        loaded = store.retrieve(wf.id)
        assert [s.id for s in loaded.stages] == [LOW, MID, HIGH], "stages came back in insertion order, not id order"
        upstream = store.get_upstream_stages(wf.id, "c")
        assert [s.id for s in upstream] == [LOW, HIGH]
        downstream = store.get_downstream_stages(wf.id, "a")
        assert [s.id for s in downstream] == [MID]

        parent = _stage("01PPPPPPPPPPPPPPPPPPPPPPPP", "p")
        second_child = _stage("01YYYYYYYYYYYYYYYYYYYYYYYY", "y", parent_stage_id=parent.id)
        first_child = _stage("01BBBBBBBBBBBBBBBBBBBBBBBB", "x", parent_stage_id=parent.id)
        first_child.status = WorkflowStatus.RUNNING
        first_child.start_time = 1
        wf2 = Workflow.create(application="order", name="w2", stages=[parent, second_child, first_child])
        store.store(wf2)
        children = store.get_synthetic_stages(wf2.id, parent.id)
        assert [s.id for s in children] == [first_child.id, second_child.id]
    finally:
        store.close()


def _workflow(store: Any, application: str, name: str, start_time: int | None, config: str = "cfg-page") -> Workflow:
    wf = Workflow.create(application=application, name=name, stages=[_stage(None, "s0")] if False else [])  # type: ignore[arg-type]
    wf.stages = [StageExecution(ref_id="s0", type="noop", name="S0", tasks=[_task()])]
    wf.pipeline_config_id = config
    wf.status = WorkflowStatus.RUNNING if start_time is not None else WorkflowStatus.NOT_STARTED
    wf.start_time = start_time
    store.store(wf)
    return wf


def test_listings_put_unstarted_last_break_ties_by_id_and_page_by_cursor(any_db: str) -> None:
    from stabilize.persistence.store import WorkflowCriteria

    store = _store(any_db)
    try:
        a = _workflow(store, "paging", "a", 100)
        b = _workflow(store, "paging", "b", 100)
        c = _workflow(store, "paging", "c", None)
        d = _workflow(store, "paging", "d", 50)
        e = _workflow(store, "paging", "e", None)
        expected = [
            max(a, b, key=lambda w: w.id).id,
            min(a, b, key=lambda w: w.id).id,
            d.id,
            max(c, e, key=lambda w: w.id).id,
            min(c, e, key=lambda w: w.id).id,
        ]

        listed = [w.id for w in store.retrieve_by_application("paging", WorkflowCriteria(page_size=10))]
        assert listed == expected, f"order was {listed}; unstarted runs must come last and ties break by id"

        seen: list[str] = []
        after: str | None = None
        for _ in range(4):
            page = list(store.retrieve_by_application("paging", WorkflowCriteria(page_size=2, after=after)))
            if not page:
                break
            seen.extend(w.id for w in page)
            after = WorkflowCriteria.cursor_after(page[-1])
        assert seen == expected, "paging by cursor skipped or repeated a run"

        by_config = [w.id for w in store.retrieve_by_pipeline_config_id("cfg-page", WorkflowCriteria(page_size=10))]
        assert by_config == expected
        pending = [w.id for w in store.get_all_pending_workflows(WorkflowCriteria(page_size=10))]
        assert pending == expected
        with pytest.raises(ValueError, match="cursor"):
            WorkflowCriteria(after="v0:nonsense").validate()
    finally:
        store.close()


def test_time_windows_exclude_unstarted_runs_unless_asked(any_db: str) -> None:
    from stabilize.persistence.store import WorkflowCriteria

    store = _store(any_db)
    try:
        early = _workflow(store, "windows", "early", 100, config="cfg-win")
        late = _workflow(store, "windows", "late", 300, config="cfg-win")
        unstarted = _workflow(store, "windows", "unstarted", None, config="cfg-win")
        after_200 = {w.id for w in store.retrieve_by_application("windows", WorkflowCriteria(start_time_after=200))}
        assert after_200 == {late.id}, "a run with no start_time was counted inside the window"
        before_200 = {w.id for w in store.retrieve_by_application("windows", WorkflowCriteria(start_time_before=200))}
        assert before_200 == {early.id}
        with_unstarted = {
            w.id
            for w in store.retrieve_by_application(
                "windows", WorkflowCriteria(start_time_after=200, include_unstarted=True)
            )
        }
        assert with_unstarted == {late.id, unstarted.id}
        assert "include_unstarted" in store.supported_workflow_criteria()
    finally:
        store.close()


def test_factories_take_schema_options_tenant_and_budget_and_tell_the_truth(db: str, tmp_path: Path) -> None:
    from stabilize.persistence.factory import create_queue, create_repository
    from stabilize.persistence.pool_options import PoolOptions

    options = PoolOptions(min_size=1, max_size=3)
    store = create_repository(db, schema="public", options=options)
    queue = create_queue(db, schema="public", options=options, store_retry_budget=timedelta(minutes=5))
    try:
        assert store.schema == "public"
        assert queue.schema == "public"
        assert queue.store_retry_budget == timedelta(minutes=5)
        assert store.is_healthy()
    finally:
        queue.close()
        store.close()
    sqlite_store = create_repository(f"sqlite:///{tmp_path / 'f.db'}", create_tables=True, tenant="")
    sqlite_store.close()
    assert "stabilize mg-up" in (create_repository.__doc__ or "")
    assert "stabilize migrate" not in (create_repository.__doc__ or "")
    assert "dead-letter" in (create_queue.__doc__ or "")
    assert "dropping" not in (create_queue.__doc__ or "")
