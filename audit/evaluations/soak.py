"""Sustained run: a threaded processor on PostgreSQL for SOAK_MINUTES (default 30).

Workflows of three stages (a fan-out with one polling task) are started at a
steady rate while the processor runs with retention enabled. Every 30 s one
JSON line reports: workflows started and completed, completed per second,
queue depth, processed_messages rows, server connections held by this
process's pools, and RSS. Measurement queries read pg_stat_activity and
COUNT(*) directly; they measure, they verify nothing.

Pass criteria, checked at the end:
  A  every started workflow reaches SUCCEEDED within 120 s of the last start
  B  RSS at the end is within 50 % of RSS at minute 5
  C  connections never exceed the pools' configured maximum
  D  processed_messages stays bounded: rows at the end <= 3 x rows at minute 10
     (retention max age 3 min, sweep every 30 s)

    STABILIZE_PROBE_DSN=... python audit/evaluations/soak.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import timedelta
from typing import Any

import psutil
import psycopg

logging.basicConfig(level=logging.ERROR)

from stabilize import (  # noqa: E402
    Orchestrator,
    PostgresQueue,
    PostgresWorkflowStore,
    QueueProcessor,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.tasks.interface import RetryableTask, Task  # noqa: E402


class Work(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={stage.ref_id: True})


class Poll(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=10)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=200)

    def execute(self, stage: StageExecution) -> TaskResult:
        polls = int(stage.context.get("polls", 0)) + 1
        if polls < 3:
            return TaskResult.running(context={"polls": polls})
        return TaskResult.success()


def _wf(i: int) -> Workflow:
    def st(ref: str, impl: str, needs: set[str]) -> StageExecution:
        return StageExecution(
            ref_id=ref,
            type="soak",
            name=ref,
            requisite_stage_ref_ids=needs,
            tasks=[TaskExecution.create(impl, impl, stage_start=True, stage_end=True)],
        )

    return Workflow.create(
        application="soak",
        name=f"soak-{i}",
        stages=[st("a", "work", set()), st("b", "poll", {"a"}), st("c", "work", {"a"}), st("d", "work", {"b", "c"})],
    )


def _scalar(dsn: str, sql: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(sql).fetchone()
    return int(row[0]) if row else 0


def main() -> int:
    dsn = os.environ["STABILIZE_PROBE_DSN"]
    minutes = float(os.environ.get("SOAK_MINUTES", "30"))
    rate = float(os.environ.get("SOAK_RATE_PER_S", "4"))
    from stabilize.cli.commands import mg_up

    mg_up(dsn)
    store = PostgresWorkflowStore(dsn)
    queue = PostgresQueue(dsn, table_name="queue_messages")
    queue.clear()
    registry = TaskRegistry()
    registry.register("work", Work)
    registry.register("poll", Poll)
    proc = QueueProcessor(queue, store=store, task_registry=registry)
    proc.config.max_workers = 8
    proc.config.retry_delay = timedelta(milliseconds=200)
    proc.config.retention_sweep_interval_seconds = 30
    proc.config.processed_messages_max_age_hours = 0.05
    orch = Orchestrator(queue, store=store)
    me = psutil.Process()
    ids: list[str] = []
    samples: list[dict[str, Any]] = []
    started_at = time.monotonic()
    next_sample = started_at + 30
    proc.start()
    try:
        i = 0
        while time.monotonic() - started_at < minutes * 60:
            wf = _wf(i)
            orch.start(wf)
            ids.append(wf.id)
            i += 1
            time.sleep(1.0 / rate)
            if time.monotonic() >= next_sample:
                next_sample += 30
                done = _scalar(dsn, "SELECT COUNT(*) FROM pipeline_executions WHERE application='soak' AND status='SUCCEEDED'")
                sample = {
                    "t_s": round(time.monotonic() - started_at),
                    "started": len(ids),
                    "succeeded": done,
                    "succeeded_per_s": round(done / (time.monotonic() - started_at), 2),
                    "queue_depth": queue.size(),
                    "processed_rows": _scalar(dsn, "SELECT COUNT(*) FROM processed_messages"),
                    "connections": _scalar(
                        dsn, "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"
                    ),
                    "rss_mb": round(me.memory_info().rss / 2**20, 1),
                    "retention_failures": getattr(proc, "_retention_failures", None),
                }
                samples.append(sample)
                print(json.dumps(sample), flush=True)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if _scalar(dsn, "SELECT COUNT(*) FROM pipeline_executions WHERE application='soak' AND status='SUCCEEDED'") >= len(ids):
                break
            time.sleep(2)
    finally:
        proc.stop(wait=True, timeout=30)
    statuses = [store.retrieve(w).status.name for w in ids[-200:]]
    succeeded = _scalar(dsn, "SELECT COUNT(*) FROM pipeline_executions WHERE application='soak' AND status='SUCCEEDED'")
    at5 = next((s for s in samples if s["t_s"] >= 300), samples[0] if samples else None)
    at10 = next((s for s in samples if s["t_s"] >= 600), samples[-1] if samples else None)
    last = samples[-1] if samples else None
    pool_max = 2 * 15 + 4
    checks = [
        (succeeded >= len(ids), "A all started workflows SUCCEEDED", f"{succeeded}/{len(ids)}; last 200 statuses {sorted(set(statuses))}"),
        (bool(at5 and last and last["rss_mb"] <= 1.5 * at5["rss_mb"]), "B RSS bounded", f"min5={at5 and at5['rss_mb']} end={last and last['rss_mb']} MB"),
        (all(s["connections"] <= pool_max for s in samples), "C connections bounded", f"max={max((s['connections'] for s in samples), default=0)} (limit {pool_max})"),
        (bool(at10 and last and last["processed_rows"] <= 3 * max(at10["processed_rows"], 1)), "D processed_messages bounded", f"min10={at10 and at10['processed_rows']} end={last and last['processed_rows']}"),
    ]
    for ok, label, detail in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")
    bad = [label for ok, label, _ in checks if not ok]
    print("VERDICT: " + ("PASS" if not bad else f"FAIL — {', '.join(bad)}"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
