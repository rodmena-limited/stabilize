"""The processed-message retention sweep does not stop message processing, and it drains a backlog.

The retention sweep (QueueProcessorConfig.retention_sweep_interval_seconds)
deletes processed_messages rows older than processed_messages_max_age_hours. It
runs on the processor's poll thread. A deployment that enables it late has a
large backlog (ci.rodmena.co.uk: 3.28 M stale rows of 3.9 M). While the sweep
runs, a newly started workflow must still be handled, and repeated sweeps must
remove the whole backlog and nothing recent.

Fixture input: N rows backdated 200 h (PostgreSQL N=2,000,000; SQLite N=500,000)
inserted directly, the way the "manipulate the input" rule prescribes for
retention paths; every assertion goes through the store's own API.

  A  a workflow started 50 ms after the first sweep is due completes within 2.0 s
  B  repeated sweeps remove the backlog: sampled stale ids are no longer processed
  C  CONTROL a message marked processed just now is still processed

    python audit/evaluations/probe_retention_does_not_stall.py   (AUDIT_ALLOW_DESTRUCTIVE=1 for PostgreSQL)
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _engine import stage, start, workflow  # noqa: E402

from stabilize import QueueProcessor, StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.tasks.interface import Task  # noqa: E402


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def _seed_pg(dsn: str, n: int) -> None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DELETE FROM processed_messages WHERE message_id LIKE 'stale-%%'")
        conn.execute(
            "INSERT INTO processed_messages (message_id, processed_at, handler_type) "
            "SELECT 'stale-' || g, NOW() - INTERVAL '200 hours', 'RunTask' FROM generate_series(1, %s) g",
            (n,),
        )


def _seed_sqlite(path: str, n: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < ?) "
        "INSERT INTO processed_messages (message_id, processed_at, handler_type) "
        "SELECT 'stale-' || x, datetime('now', '-200 hours'), 'RunTask' FROM g",
        (n,),
    )
    conn.commit()
    conn.close()


def _case(store: Any, queue: Any, n: int) -> tuple[float, bool, bool]:
    registry = TaskRegistry()
    registry.register("ok", Ok)
    store.mark_message_processed(message_id="fresh-1", handler_type="RunTask", execution_id=None)
    proc = QueueProcessor(queue, store=store, task_registry=registry)
    proc.config.retry_delay = timedelta(milliseconds=50)
    proc.config.retention_sweep_interval_seconds = 0.5
    proc.config.processed_messages_max_age_hours = 1.0
    proc.config.poll_frequency_ms = 10
    wf = workflow([stage("s", ["ok"])])
    proc.start()
    try:
        time.sleep(0.55)
        t0 = time.monotonic()
        start(store, queue, wf)
        latency = 99.0
        while time.monotonic() - t0 < 60:
            if store.retrieve(wf.id).status.is_complete:
                latency = time.monotonic() - t0
                break
            time.sleep(0.01)
        samples = [f"stale-{i}" for i in (1, n // 2, n)]
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and any(store.is_message_processed(s) for s in samples):
            time.sleep(0.5)
        drained = not any(store.is_message_processed(s) for s in samples)
    finally:
        proc.stop(wait=True, timeout=30)
    fresh = store.is_message_processed("fresh-1")
    return latency, drained, fresh


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    runs: list[tuple[str, Any]] = []

    def sqlite_run() -> tuple[float, bool, bool]:
        from stabilize import SqliteQueue, SqliteWorkflowStore

        with tempfile.TemporaryDirectory() as d:
            path = f"{d}/probe.db"
            store = SqliteWorkflowStore(f"sqlite:///{path}", create_tables=True)
            queue = SqliteQueue(f"sqlite:///{path}", table_name="queue_messages")
            queue._create_table()
            _seed_sqlite(path, 500_000)
            try:
                return _case(store, queue, 500_000)
            finally:
                queue.close()
                store.close()

    runs.append(("sqlite", sqlite_run))
    from _backends import postgres_dsn

    with postgres_dsn() as dsn:
        if dsn is None or os.environ.get("AUDIT_ALLOW_DESTRUCTIVE") != "1":
            print("[SKIP] postgres: needs STABILIZE_PROBE_DSN or Docker, and AUDIT_ALLOW_DESTRUCTIVE=1 (2 M row fixture)")
        else:

            def pg_run() -> tuple[float, bool, bool]:
                from stabilize import PostgresQueue, PostgresWorkflowStore

                _seed_pg(dsn, 2_000_000)
                store = PostgresWorkflowStore(dsn)
                queue = PostgresQueue(dsn, table_name="queue_messages")
                queue.clear()
                try:
                    return _case(store, queue, 2_000_000)
                finally:
                    queue.clear()
                    queue.close()
                    store.close()

            runs.append(("postgres", pg_run))
        for name, fn in runs:
            latency, drained, fresh = fn()
            for ok, label, detail in (
                (latency <= 2.0, "A workflow during sweep", f"completed in {latency:.2f} s (want <= 2.0 s)"),
                (drained, "B backlog drained", f"sampled stale ids gone={drained}"),
                (fresh, "C CONTROL recent record kept", f"fresh-1 processed={fresh}"),
            ):
                print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {detail}")
                if not ok:
                    failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — retention neither stalls processing nor leaves a backlog")
    return 0


if __name__ == "__main__":
    sys.exit(main())
