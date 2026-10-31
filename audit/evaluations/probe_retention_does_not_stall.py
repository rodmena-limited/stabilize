"""The processed-message retention sweep does not stop message processing, and it drains a backlog.

The retention sweep (QueueProcessorConfig.retention_sweep_interval_seconds)
deletes processed_messages rows older than processed_messages_max_age_hours. It
runs on the processor's poll thread. A deployment that enables it late has a
large backlog (ci.rodmena.co.uk: 3.28 M stale rows of 3.9 M). While the sweep
runs, a newly started workflow must still be handled, and repeated sweeps must
remove the whole backlog and nothing recent.

Fixture input: N rows backdated 200 h (PostgreSQL N=2,000,000; SQLite N=500,000)
keyed like queue_messages delivery keys (numeric row ids, above any real row,
unique to this run) inserted directly, the way the "manipulate the input" rule prescribes for
retention paths; every assertion goes through the store's own API.

  A  a workflow started 50 ms after the first sweep is due completes within 2.0 s
  B  repeated sweeps remove the backlog: sampled stale ids are no longer processed
  C  CONTROL a message marked processed just now is still processed
  F  a stale record whose message is still in the queue is kept (only gone messages are removed)
  D  store.cleanup_old_processed_messages(max_age_hours=168) (int) succeeds
  E  store.cleanup_old_processed_messages(max_age_hours=1.0) (float, the config default type) succeeds

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


BASE = 10**15 + (int(time.time()) % 10**6) * 10**7


def _key(i: int) -> str:
    return str(BASE + i)


def _seed_pg(dsn: str, n: int) -> None:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO processed_messages (message_id, processed_at, handler_type) "
            "SELECT (%s::bigint + g)::text, NOW() - INTERVAL '200 hours', 'RunTask' FROM generate_series(1, %s) g",
            (BASE, n),
        )


def _seed_sqlite(path: str, n: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "WITH RECURSIVE g(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM g WHERE x < ?) "
        "INSERT INTO processed_messages (message_id, processed_at, handler_type) "
        "SELECT CAST(? + x AS TEXT), datetime('now', '-200 hours'), 'RunTask' FROM g",
        (n, BASE),
    )
    conn.commit()
    conn.close()


DIRECT: dict[str, str] = {}


class _Warnings(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage()[:200])


def _seed_live_record(store: Any, queue: Any) -> str:
    from stabilize.queue.messages import StartWorkflow

    queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="retention-probe-live"), timedelta(hours=1))
    url = store.connection_string
    if url.startswith("sqlite"):
        conn = sqlite3.connect(url.split("sqlite:///", 1)[1])
        row_id = conn.execute(f"SELECT MAX(id) FROM {queue.table_name}").fetchone()[0]
        conn.execute(
            "INSERT INTO processed_messages (message_id, processed_at, handler_type) "
            "VALUES (?, datetime('now', '-300 hours'), 'RunTask')",
            (str(row_id),),
        )
        conn.commit()
        conn.close()
        return str(row_id)
    import psycopg

    with psycopg.connect(url, autocommit=True) as conn:
        row_id = conn.execute(f"SELECT MAX(id) FROM {queue.table_name}").fetchone()[0]
        conn.execute(
            "INSERT INTO processed_messages (message_id, processed_at, handler_type) "
            "VALUES (%s, NOW() - INTERVAL '300 hours', 'RunTask')",
            (str(row_id),),
        )
    return str(row_id)


def _case(store: Any, queue: Any, n: int) -> tuple[float, bool, bool, bool]:
    warnings = _Warnings()
    proc_logger = logging.getLogger("stabilize.queue.processor.processor")
    proc_logger.addHandler(warnings)
    proc_logger.setLevel(logging.INFO)
    registry = TaskRegistry()
    registry.register("ok", Ok)
    live_key = _seed_live_record(store, queue)
    fresh_key = _key(n + 1)
    store.mark_message_processed(message_id=fresh_key, handler_type="RunTask", execution_id=None)
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
        samples = [_key(i) for i in (1, n // 2, n)]
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and any(store.is_message_processed(s) for s in samples):
            time.sleep(0.5)
        drained = not any(store.is_message_processed(s) for s in samples)
        live_kept = store.is_message_processed(live_key)
    finally:
        proc.stop(wait=True, timeout=30)
    proc_logger.removeHandler(warnings)
    print(f"[INFO] processor warnings during the run: {[w for w in warnings.lines if 'etention' in w][:5]}")
    for hours in (168, 1.0):
        t1 = time.monotonic()
        try:
            removed = store.cleanup_old_processed_messages(max_age_hours=hours)
            DIRECT[repr(hours)] = f"removed {removed} in {time.monotonic() - t1:.2f} s"
        except Exception as e:
            DIRECT[repr(hours)] = f"raised {type(e).__name__}: {str(e).splitlines()[0][:120]}"
    fresh = store.is_message_processed(fresh_key)
    return latency, drained, fresh, live_kept


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    runs: list[tuple[str, Any]] = []

    def sqlite_run() -> tuple[float, bool, bool, bool]:
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
        if os.environ.get("AUDIT_ALLOW_DESTRUCTIVE") != "1":
            print("[SKIP] postgres: needs AUDIT_ALLOW_DESTRUCTIVE=1 (2 M row fixture)")
        else:

            def pg_run() -> tuple[float, bool, bool, bool]:
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
            DIRECT.clear()
            latency, drained, fresh, live_kept = fn()
            for ok, label, detail in (
                (latency <= 2.0, "A workflow during sweep", f"completed in {latency:.2f} s (want <= 2.0 s)"),
                (drained, "B backlog drained", f"sampled stale ids gone={drained}"),
                (fresh, "C CONTROL recent record kept", f"recent record processed={fresh}"),
                (live_kept, "F record of a still-queued message kept", f"processed={live_kept}"),
                (DIRECT.get("168", "").startswith("removed"), "D direct cleanup max_age_hours=168", DIRECT.get("168", "")),
                (DIRECT.get("1.0", "").startswith("removed"), "E direct cleanup max_age_hours=1.0", DIRECT.get("1.0", "")),
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
