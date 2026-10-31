"""WI-6: StageCriteria queries on both backends, with the page served by the tenant-status index."""

from __future__ import annotations

import json
import statistics
import time
from collections.abc import Iterator
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
    with scratch_database(postgres_container, "mgstq") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'stages.db'}"
        return
    yield request.getfixturevalue("db")


def _store(url: str) -> Any:
    if url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore

        return SqliteWorkflowStore(url, create_tables=True)
    from stabilize.persistence.postgres import PostgresWorkflowStore

    return PostgresWorkflowStore(url)


def _workflow(store: Any, application: str, name: str, specs: list[tuple[Any, ...]]) -> Workflow:
    stages = []
    for ref, kind, status, context, start, end in specs:
        task = TaskExecution.create(name=f"{ref}-t", implementing_class="noop", stage_start=True, stage_end=True)
        stage = StageExecution(ref_id=ref, type=kind, name=ref.upper(), tasks=[task], context=dict(context))
        stage.status = status
        stage.start_time = start
        stage.end_time = end
        stages.append(stage)
    workflow = Workflow.create(application=application, name=name, stages=stages)
    workflow.status = WorkflowStatus.RUNNING
    store.store(workflow)
    return workflow


def _seed(store: Any) -> dict[str, Any]:
    now = int(time.time() * 1000)
    s = WorkflowStatus
    a = _workflow(
        store,
        "billing",
        "a",
        [
            ("s0", "noop", s.SUCCEEDED, {"env": "prod", "n": 3, "flag": True}, now - 5000, now - 4000),
            ("s1", "wait", s.RUNNING, {"env": "prod"}, now - 3000, None),
            ("s2", "noop", s.NOT_STARTED, {}, None, None),
        ],
    )
    b = _workflow(
        store,
        "billing",
        "b",
        [
            ("s0", "noop", s.TERMINAL, {"env": "staging", "n": 3}, now - 2000, now - 1000),
            ("s1", "wait", s.SUSPENDED, {"env": "prod", "nested": {"k": 1}}, now - 900, None),
        ],
    )
    c = _workflow(store, "ops", "c", [("s0", "noop", s.RUNNING, {"env": "prod", "n": 4}, now - 800, None)])
    return {"a": a, "b": b, "c": c, "now": now}


def test_filters_compose_and_pages_descend_by_id(any_db: str) -> None:
    from stabilize.persistence.store import StageCriteria

    store = _store(any_db)
    try:
        seeded = _seed(store)
        a, b, c = seeded["a"], seeded["b"], seeded["c"]
        running = {a.stages[1].id, c.stages[0].id}
        got = store.retrieve_stages(StageCriteria(statuses={WorkflowStatus.RUNNING}))
        assert {s.id for s in got} == running
        assert [s.id for s in got] == sorted(running, reverse=True)
        assert store.count_stages(StageCriteria(statuses={WorkflowStatus.RUNNING})) == 2

        got = store.retrieve_stages(StageCriteria(types={"wait"}, ref_ids={"s1"}))
        assert {s.id for s in got} == {a.stages[1].id, b.stages[1].id}
        got = store.retrieve_stages(StageCriteria(application="ops"))
        assert [s.id for s in got] == [c.stages[0].id]
        assert store.count_stages(StageCriteria(application="billing", statuses={WorkflowStatus.NOT_STARTED})) == 1
        assert store.count_stages(StageCriteria(execution_id=a.id)) == 3

        first = store.retrieve_stages(StageCriteria(page_size=4))
        assert len(first) == 4
        second = store.retrieve_stages(StageCriteria(page_size=4, after_id=first[-1].id))
        assert len(second) == 2
        ids = [s.id for s in first + second]
        assert ids == sorted(ids, reverse=True) and len(set(ids)) == 6
        assert store.retrieve_stages(StageCriteria(page_size=4, after_id=second[-1].id)) == []
        assert store.count_stages(StageCriteria()) == 6
        assert store.supported_criteria() >= {"statuses", "types", "ref_ids", "application", "context_contains"}
    finally:
        store.close()


def test_time_windows_exclude_stages_without_the_time(any_db: str) -> None:
    from stabilize.persistence.store import StageCriteria

    store = _store(any_db)
    try:
        seeded = _seed(store)
        a, b, c, now = seeded["a"], seeded["b"], seeded["c"], seeded["now"]
        started = store.retrieve_stages(StageCriteria(start_time_after=now - 3500))
        assert {s.id for s in started} == {a.stages[1].id, b.stages[0].id, b.stages[1].id, c.stages[0].id}
        assert store.count_stages(StageCriteria(start_time_before=now)) == 5, "a stage with no start_time was counted"
        ended = store.retrieve_stages(StageCriteria(end_time_before=now - 2000))
        assert {s.id for s in ended} == {a.stages[0].id}
        assert store.count_stages(StageCriteria(end_time_after=now - 10_000)) == 2
    finally:
        store.close()


def test_context_containment_matches_top_level_scalars_and_refuses_the_rest(any_db: str) -> None:
    from stabilize.persistence.store import StageCriteria

    store = _store(any_db)
    try:
        seeded = _seed(store)
        a, b, c = seeded["a"], seeded["b"], seeded["c"]
        got = store.retrieve_stages(StageCriteria(context_contains={"env": "prod", "n": 3}))
        assert {s.id for s in got} == {a.stages[0].id}
        got = store.retrieve_stages(StageCriteria(context_contains={"flag": True}))
        assert {s.id for s in got} == {a.stages[0].id}
        got = store.retrieve_stages(StageCriteria(context_contains={"env": "prod"}))
        assert {s.id for s in got} == {a.stages[0].id, a.stages[1].id, b.stages[1].id, c.stages[0].id}
        assert store.count_stages(StageCriteria(context_contains={"env": "qa"})) == 0
        with pytest.raises(ValueError, match="nested"):
            StageCriteria(context_contains={"nested": {"k": 1}})
        with pytest.raises(ValueError, match="page_size"):
            StageCriteria(page_size=501)
        with pytest.raises(ValueError, match="page_size"):
            StageCriteria(page_size=0)
    finally:
        store.close()


def test_results_carry_tasks_and_one_summary_parent_per_workflow(any_db: str) -> None:
    from stabilize.persistence.store import StageCriteria

    store = _store(any_db)
    try:
        seeded = _seed(store)
        a, c = seeded["a"], seeded["c"]
        [stage] = store.retrieve_stages(StageCriteria(execution_id=c.id))
        assert [t.name for t in stage.tasks] == ["s0-t"]
        assert stage.has_execution()
        assert stage.execution.id == c.id and stage.execution.application == "ops"
        assert stage.execution.stages == [], "the parent is a summary, not the materialised workflow"
        a_stages = store.retrieve_stages(StageCriteria(execution_id=a.id))
        assert len(a_stages) == 3
        assert len({id(s.execution) for s in a_stages}) == 1, "the stages of one workflow share one parent object"
        assert all(len(s.tasks) == 1 for s in a_stages)
    finally:
        store.close()


def test_a_status_page_is_served_by_the_tenant_status_index(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.persistence.postgres.stage_query import stage_query_sql
    from stabilize.persistence.store import StageCriteria

    store = PostgresWorkflowStore(db)
    try:
        for i in range(400):
            status = WorkflowStatus.RUNNING if i % 40 == 0 else WorkflowStatus.SUCCEEDED
            _workflow(
                store,
                "load",
                f"w{i}",
                [
                    ("s0", "noop", WorkflowStatus.SUCCEEDED, {"i": i}, 1, 2),
                    ("s1", "noop", status, {"i": i}, 3, None if status is WorkflowStatus.RUNNING else 4),
                    ("s2", "noop", WorkflowStatus.SUCCEEDED, {}, 5, 6),
                ],
            )
        criteria = StageCriteria(statuses={WorkflowStatus.RUNNING}, page_size=50)
        sql, params = stage_query_sql(store, criteria)
        with psycopg.connect(db) as conn:
            conn.execute("ANALYZE stage_executions")
            plan = conn.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, params).fetchone()[0]
        text = json.dumps(plan)
        assert "idx_stages_tenant_status_id" in text, text
        removed = sum(int(n) for n in _walk(plan, "Rows Removed by Filter"))
        assert removed <= 50, f"the page scanned past its rows: {removed} removed by filter\n{text}"

        samples = []
        for _ in range(20):
            started = time.perf_counter()
            page = store.retrieve_stages(criteria)
            samples.append((time.perf_counter() - started) * 1000)
        assert len(page) == 10
        p95 = statistics.quantiles(samples, n=20)[18]
        print(f"stage query p95 {p95:.1f} ms over 20 repeats of a 10-row status page among 1200 stages")
    finally:
        store.close()


def _walk(node: Any, key: str) -> Iterator[Any]:
    if isinstance(node, dict):
        if key in node:
            yield node[key]
        for value in node.values():
            yield from _walk(value, key)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item, key)
