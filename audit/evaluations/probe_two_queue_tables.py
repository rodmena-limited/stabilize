"""Two queue tables sharing one store each deliver every message.

QueueProcessor marks each handled message in the store-wide processed_messages
table and skips a message whose id is already there. Two queues on different
tables (QueueProcessor/PostgresQueue/SqliteQueue table_name) keep their own
row-id sequences, so their ids overlap. A message on table B whose row id was
already handled from table A must still be handled.

Workflow WA is started on queue_messages and WB on q_b; processor A polls
queue_messages, processor B polls q_b. Each workflow has one task.

  A  own id sequence per table: WA and WB both SUCCEEDED, each task executed once
  B  CONTROL (PostgreSQL only): q_b shares queue_messages' sequence, so ids never
     overlap: both SUCCEEDED

    python audit/evaluations/probe_two_queue_tables.py
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.tasks.interface import Task  # noqa: E402

COUNT: dict[str, int] = {}


class Count(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        COUNT[stage.execution.id] = COUNT.get(stage.execution.id, 0) + 1
        return TaskResult.success()


def _wf(app: str) -> Workflow:
    return Workflow.create(
        application=app,
        name=app,
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("count", "count", stage_start=True, stage_end=True)],
            )
        ],
    )


def _run(store: Any, qa: Any, qb: Any) -> tuple[Any, Any]:
    registry = TaskRegistry()
    registry.register("count", Count)
    procs = []
    for q in (qa, qb):
        p = QueueProcessor(q, store=store, task_registry=registry)
        p.config.retry_delay = timedelta(milliseconds=50)
        procs.append(p)
    wa, wb = _wf("app-a"), _wf("app-b")
    for wf, q in ((wa, qa), (wb, qb)):
        store.store(wf)
        Orchestrator(q, store=store).start(wf)
    deadline = time.monotonic() + 10
    idle_since = time.monotonic()
    while time.monotonic() < deadline:
        worked = False
        for p in procs:
            try:
                worked = p.process_one() or worked
            except Exception:
                worked = True
        if worked:
            idle_since = time.monotonic()
        elif time.monotonic() - idle_since > 1.0:
            break
        else:
            time.sleep(0.02)
    return store.retrieve(wa.id), store.retrieve(wb.id)


def _sqlite_case() -> tuple[Any, Any]:
    from stabilize import SqliteQueue, SqliteWorkflowStore

    with tempfile.TemporaryDirectory() as d:
        url = f"sqlite:///{d}/probe.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        qa = SqliteQueue(url, table_name="queue_messages")
        qb = SqliteQueue(url, table_name="q_b")
        qa._create_table()
        qb._create_table()
        try:
            return _run(store, qa, qb)
        finally:
            qa.close()
            qb.close()
            store.close()


def _pg_case(dsn: str, shared: bool) -> tuple[Any, Any]:
    import psycopg

    from stabilize import PostgresQueue, PostgresWorkflowStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS q_b_dlq, q_b")
        conn.execute("DROP SEQUENCE IF EXISTS q_b_id_seq")
        conn.execute("CREATE TABLE q_b (LIKE queue_messages INCLUDING ALL)")
        conn.execute("CREATE TABLE q_b_dlq (LIKE queue_messages_dlq INCLUDING ALL)")
        if not shared:
            conn.execute("CREATE SEQUENCE q_b_id_seq AS BIGINT OWNED BY q_b.id")
            conn.execute("ALTER TABLE q_b ALTER COLUMN id SET DEFAULT nextval('q_b_id_seq')")
    store = PostgresWorkflowStore(dsn)
    qa = PostgresQueue(dsn, table_name="queue_messages")
    qb = PostgresQueue(dsn, table_name="q_b")
    qa.clear()
    qb.clear()
    try:
        return _run(store, qa, qb)
    finally:
        qa.close()
        qb.close()
        store.close()


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    from _backends import postgres_dsn

    cases: list[tuple[str, Any]] = [("sqlite A own id sequence per table", _sqlite_case)]
    failures: list[str] = []
    with postgres_dsn() as dsn:
        cases.append(("postgres A own id sequence per table", lambda: _pg_case(dsn, shared=False)))
        cases.append(("postgres B CONTROL shared sequence", lambda: _pg_case(dsn, shared=True)))
        for label, fn in cases:
            COUNT.clear()
            wa, wb = fn()
            ok = (
                wa.status.name == "SUCCEEDED"
                and wb.status.name == "SUCCEEDED"
                and COUNT.get(wa.id) == 1
                and COUNT.get(wb.id) == 1
            )
            print(
                f"[{'PASS' if ok else 'FAIL'}] {label}: WA={wa.status.name} x{COUNT.get(wa.id, 0)} "
                f"WB={wb.status.name} x{COUNT.get(wb.id, 0)} (want SUCCEEDED x1 each)"
            )
            if not ok:
                failures.append(label)
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — every message on both tables was handled")
    return 0


if __name__ == "__main__":
    sys.exit(main())
