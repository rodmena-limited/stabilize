from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from stabilize import StageExecution, TaskExecution, Workflow, WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartStage

FILLER = 20000
OLD_MS = int(time.time() * 1000) - 30 * 86400 * 1000


def _completed_workflow(application: str) -> Workflow:
    wf = Workflow.create(
        application=application,
        name="plan",
        stages=[
            StageExecution(
                ref_id="s1",
                type="noop",
                name="s1",
                tasks=[TaskExecution.create(name="t", implementing_class="noop", stage_start=True, stage_end=True)],
            )
        ],
    )
    wf.status = WorkflowStatus.SUCCEEDED
    wf.start_time = OLD_MS - 1000
    wf.end_time = OLD_MS
    return wf


def _calls(store: WorkflowStore, queue: Queue, wf: Workflow) -> list[tuple[str, Callable[[], Any]]]:
    stage_id = wf.stages[0].id
    return [
        ("completed_workflow_ids[application]", lambda: store.completed_workflow_ids(OLD_MS + 1, 200, "plan-app")),
        ("completed_workflow_ids[all]", lambda: store.completed_workflow_ids(OLD_MS + 1, 200)),
        ("has_pending_message_for_execution", lambda: queue.has_pending_message_for_execution(wf.id)),
        ("has_pending_message_for_stage", lambda: queue.has_pending_message_for_stage(stage_id)),
        ("purge_workflow", lambda: queue.purge_workflow(wf.id)),
    ]


def _seed_postgres(store: Any, queue: Any) -> None:
    with store._pool.connection() as conn:
        conn.execute(
            "INSERT INTO pipeline_executions (id, type, application, status, start_time, end_time) "
            "SELECT 'F' || lpad(g::text, 25, '0'), 'PIPELINE', 'app-' || (g %% 30), "
            "(ARRAY['SUCCEEDED','TERMINAL','CANCELED','RUNNING'])[1 + g %% 4], %s, %s - g "
            "FROM generate_series(1, %s) g",
            (OLD_MS - 5000, OLD_MS, FILLER),
        )
        conn.commit()
    with queue._get_pool().connection() as conn:
        for table in ("queue_messages", "queue_messages_dlq"):
            extra = ", original_id" if table.endswith("_dlq") else ""
            conn.execute(
                f"INSERT INTO {table} (message_id, message_type, payload{extra}) "
                "SELECT 'filler-' || g, 'StartStage', "
                "jsonb_build_object('execution_id', 'F' || lpad(g::text, 25, '0'), 'stage_id', 'S' || g)"
                f"{', g' if extra else ''} FROM generate_series(1, %s) g",
                (FILLER,),
            )
        conn.commit()
        for table in ("pipeline_executions", "queue_messages", "queue_messages_dlq"):
            conn.execute(f"ANALYZE {table}")
        conn.commit()


def _postgres_plans(store: Any, queue: Any, wf: Workflow, monkeypatch: pytest.MonkeyPatch) -> dict[str, list[dict]]:
    import psycopg

    captured: list[tuple[str, Any]] = []
    original = psycopg.Cursor.execute

    def recording(self: Any, query: Any, params: Any = None, **kw: Any) -> Any:
        captured.append((query, params))
        return original(self, query, params, **kw)

    plans: dict[str, list[dict]] = {}
    for name, call in _calls(store, queue, wf):
        captured.clear()
        monkeypatch.setattr(psycopg.Cursor, "execute", recording)
        call()
        monkeypatch.setattr(psycopg.Cursor, "execute", original)
        statements = [(q, p) for q, p in captured if re.search(r"\b(pipeline_executions|queue_messages)", str(q))]
        assert statements, f"{name}: no statement reached the tables"
        nodes: list[dict] = []
        with store._pool.connection() as conn:
            conn.execute("SET enable_seqscan = off")
            for query, params in statements:
                literal = psycopg.ClientCursor(conn).mogrify(query, params)
                row = conn.execute(f"EXPLAIN (FORMAT JSON) {literal}").fetchone()
                plan = row[0] if not isinstance(row, dict) else next(iter(row.values()))
                nodes.extend(_walk(plan[0]["Plan"]))
            conn.rollback()
        plans[name] = nodes
    return plans


def _walk(node: dict) -> Iterator[dict]:
    yield node
    for child in node.get("Plans", []):
        yield from _walk(child)


def _sqlite_plans(store: Any, queue: Any, wf: Workflow) -> dict[str, list[str]]:
    plans: dict[str, list[str]] = {}
    conns = {id(c): c for c in (store._get_connection(), queue._get_connection())}
    for name, call in _calls(store, queue, wf):
        captured: list[tuple[Any, str]] = []
        for conn in conns.values():
            conn.set_trace_callback(lambda sql, c=conn: captured.append((c, sql)))
        try:
            call()
        finally:
            for conn in conns.values():
                conn.set_trace_callback(None)
        statements = [
            (c, s)
            for c, s in captured
            if re.match(r"\s*(SELECT|DELETE)\b", s, re.I) and re.search(r"\b(pipeline_executions|queue_messages)", s)
        ]
        assert statements, f"{name}: no statement reached the tables"
        details: list[str] = []
        for conn, sql in statements:
            details.extend(str(r[3]) for r in conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall())
        plans[name] = details
    return plans


def _seed_sqlite(store: Any, queue: Any) -> None:
    sconn = store._get_connection()
    sconn.executemany(
        "INSERT INTO pipeline_executions (id, type, application, status, start_time, end_time) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                f"F{g:025d}",
                "PIPELINE",
                f"app-{g % 30}",
                ("SUCCEEDED", "TERMINAL", "CANCELED", "RUNNING")[g % 4],
                OLD_MS - 5000,
                OLD_MS - g,
            )
            for g in range(1, FILLER + 1)
        ],
    )
    sconn.commit()
    qconn = queue._get_connection()
    for table in ("queue_messages", "queue_messages_dlq"):
        qconn.executemany(
            f"INSERT INTO {table} (message_id, message_type, payload) VALUES (?, ?, ?)",
            [
                (f"filler-{g}", "StartStage", json.dumps({"execution_id": f"F{g:025d}", "stage_id": f"S{g}"}))
                for g in range(1, FILLER + 1)
            ],
        )
    qconn.commit()


def test_purge_and_recovery_lookups_use_an_index(
    backend: str, repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    if backend == "sqlite":
        queue._create_table()
    wf = _completed_workflow("plan-app")
    repository.store(wf)
    queue.push(StartStage(execution_type=wf.type.value, execution_id=wf.id, stage_id=wf.stages[0].id))

    if backend == "postgres":
        _seed_postgres(repository, queue)
        plans = _postgres_plans(repository, queue, wf, monkeypatch)
        failures = {
            name: sorted({n["Node Type"] + ":" + n.get("Relation Name", "") for n in nodes})
            for name, nodes in plans.items()
            if any(n["Node Type"] in ("Seq Scan", "Sort", "Incremental Sort") for n in nodes)
        }
    else:
        _seed_sqlite(repository, queue)
        plans_sqlite = _sqlite_plans(repository, queue, wf)
        failures = {
            name: details
            for name, details in plans_sqlite.items()
            if any(re.fullmatch(r"SCAN \w+", d) or "TEMP B-TREE" in d for d in details)
        }
    assert failures == {}, f"[FAIL] purge_index_plan: full scan or sort in {failures}"

    assert repository.completed_workflow_ids(OLD_MS + 1, 200, "plan-app") == [wf.id]
    assert queue.has_pending_message_for_execution(wf.id) is False
