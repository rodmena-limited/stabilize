"""purge-workflows and recovery's per-workflow lookup stay fast at 1,000,000 rows.

The store holds 200 workflows that completed 30 days ago and are due for purge,
plus FILLER workflows that completed 1 day ago (not due) spread over 30
applications. The queue table holds FILLER messages of the filler workflows
and its dead-letter table FILLER / 10. The filler rows are inserted in bulk
SQL into the throwaway database; every measured call goes through the product.

  A  purge_completed_workflows(cutoff 7 days, batch 200) deletes the 200 due
     workflows within PURGE_LIMIT_S, and leaves none due afterwards
  B  the same with application= for the application holding the due workflows
  C  200 has_pending_message_for_execution calls finish within LOOKUP_LIMIT_S

    python audit/evaluations/probe_purge_scales.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402

from stabilize import StageExecution, TaskExecution, Workflow, WorkflowStatus  # noqa: E402
from stabilize.maintenance import purge_completed_workflows  # noqa: E402
from stabilize.queue.messages import StartStage  # noqa: E402

FILLER = int(os.environ.get("PROBE_PURGE_FILLER", "1000000"))
DUE = 200
PURGE_LIMIT_S = 2.0
LOOKUP_LIMIT_S = 0.5
DAY_MS = 86400 * 1000
NOW_MS = int(time.time() * 1000)
APP = "probe-due"

results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str) -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", flush=True)


def due_workflows(store, queue, application: str) -> list[str]:
    ids = []
    for i in range(DUE):
        wf = Workflow.create(
            application=application,
            name=f"due-{i}",
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
        wf.start_time = NOW_MS - 30 * DAY_MS - 1000
        wf.end_time = NOW_MS - 30 * DAY_MS + i
        store.store(wf)
        queue.push(StartStage(execution_type=wf.type.value, execution_id=wf.id, stage_id=wf.stages[0].id))
        ids.append(wf.id)
    return ids


def seed_postgres(store, queue) -> None:
    with store._pool.connection() as conn:
        conn.execute(
            "INSERT INTO pipeline_executions (id, type, application, status, start_time, end_time) "
            "SELECT 'F' || lpad(g::text, 25, '0'), 'PIPELINE', 'app-' || (g %% 30), 'SUCCEEDED', %s, %s + g "
            "FROM generate_series(1, %s) g ON CONFLICT DO NOTHING",
            (NOW_MS - 2 * DAY_MS, NOW_MS - DAY_MS, FILLER),
        )
        conn.execute(
            "INSERT INTO queue_messages (message_id, message_type, payload) "
            "SELECT 'filler-' || g, 'StartStage', jsonb_build_object('execution_id', 'F' || lpad(g::text, 25, '0')) "
            "FROM generate_series(1, %s) g ON CONFLICT DO NOTHING",
            (FILLER,),
        )
        conn.execute(
            "INSERT INTO queue_messages_dlq (message_id, message_type, payload) "
            "SELECT 'dlq-' || g, 'StartStage', jsonb_build_object('execution_id', 'F' || lpad(g::text, 25, '0')) "
            "FROM generate_series(1, %s) g "
            "WHERE NOT EXISTS (SELECT 1 FROM queue_messages_dlq WHERE message_id = 'dlq-1')",
            (FILLER // 10,),
        )
        conn.commit()
        for table in ("pipeline_executions", "queue_messages", "queue_messages_dlq"):
            conn.execute(f"ANALYZE {table}")
        conn.commit()


def seed_sqlite(store, queue) -> None:
    conn = store._get_connection()
    conn.executemany(
        "INSERT INTO pipeline_executions (id, type, application, status, start_time, end_time) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            (f"F{g:025d}", "PIPELINE", f"app-{g % 30}", "SUCCEEDED", NOW_MS - 2 * DAY_MS, NOW_MS - DAY_MS + g)
            for g in range(1, FILLER + 1)
        ),
    )
    conn.commit()
    qconn = queue._get_connection()
    for table, n in (("queue_messages", FILLER), ("queue_messages_dlq", FILLER // 10)):
        qconn.executemany(
            f"INSERT INTO {table} (message_id, message_type, payload) VALUES (?, ?, ?)",
            ((f"{table}-{g}", "StartStage", json.dumps({"execution_id": f"F{g:025d}"})) for g in range(1, n + 1)),
        )
    qconn.commit()


def run(name: str, factory) -> None:
    cutoff = NOW_MS - 7 * DAY_MS
    with factory() as (store, queue):
        (seed_postgres if name == "postgres" else seed_sqlite)(store, queue)
        for case, application in (("A", None), ("B", APP)):
            ids = due_workflows(store, queue, APP)
            other = due_workflows(store, queue, "probe-other") if application else []
            t0 = time.perf_counter()
            n = purge_completed_workflows(
                store, queue, cutoff, batch_size=DUE, max_workflows=DUE, application=application
            )
            took = time.perf_counter() - t0
            left = store.completed_workflow_ids(cutoff, 10, application)
            ok = n == DUE and left == [] and took <= PURGE_LIMIT_S
            record(
                f"{name} {case} purge {DUE} among {FILLER} rows{' application=' + APP if application else ''}",
                ok,
                f"purged={n} left_due={len(left)} took={took:.3f}s limit={PURGE_LIMIT_S}s",
            )
            if case == "A":
                t0 = time.perf_counter()
                found = sum(queue.has_pending_message_for_execution(i) for i in ids)
                took = time.perf_counter() - t0
                record(
                    f"{name} C {DUE} has_pending_message_for_execution",
                    found == 0 and took <= LOOKUP_LIMIT_S,
                    f"found={found} took={took:.3f}s limit={LOOKUP_LIMIT_S}s",
                )
            else:
                kept = store.completed_workflow_ids(cutoff, DUE + 1, "probe-other")
                record(
                    f"{name} B other application kept",
                    sorted(kept) == sorted(other),
                    f"kept={len(kept)} expected={len(other)}",
                )
                purge_completed_workflows(store, queue, cutoff, application="probe-other")


def main() -> int:
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no database available", flush=True)
            continue
        run(name, factory)
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    return 1 if failed or not results else 0


if __name__ == "__main__":
    sys.exit(main())
