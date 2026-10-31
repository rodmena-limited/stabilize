"""A recovery sweep does not re-issue work that is in flight on another queue table.

Recovery skips a RUNNING task when a message for it is already queued. With two
queue tables, a processor's sweep that is not filtered by application
(recovery_application=None, the default) also visits workflows that run on the
other table, and must not push a second RunTask for their in-flight tasks into
its own table: that RunTask runs the task a second time, under this
processor's task registry.

Workflow WB (application app-b) is started on table q_b and stepped until its
task is RUNNING with its RunTask on q_b. Processor A (queue_messages) then runs
one recovery sweep.

  A  recovery_application=None: queue_messages holds no message for WB's task
  B  CONTROL recovery_application="app-a": queue_messages holds no message for WB's task

    python audit/evaluations/probe_recovery_cross_table.py
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
from stabilize.models.status import WorkflowStatus  # noqa: E402
from stabilize.tasks.interface import RetryableTask  # noqa: E402


class Poll(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(minutes=1)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()


def _case(store: Any, qa: Any, qb: Any, app_filter: str | None) -> tuple[bool, str]:
    registry = TaskRegistry()
    registry.register("poll", Poll)
    pb = QueueProcessor(qb, store=store, task_registry=registry)
    pb.config.retry_delay = timedelta(milliseconds=50)
    pa = QueueProcessor(qa, store=store, task_registry=registry)
    pa.config.recovery_application = app_filter
    wb = Workflow.create(
        application="app-b",
        name="wb",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("poll", "poll", stage_start=True, stage_end=True)],
            )
        ],
    )
    store.store(wb)
    Orchestrator(qb, store=store).start(wb)
    task_id = wb.stages[0].tasks[0].id
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        t = store.retrieve(wb.id).stages[0].tasks[0]
        if t.status == WorkflowStatus.RUNNING and qb.has_pending_message_for_task(task_id):
            break
        try:
            if not pb.process_one():
                time.sleep(0.02)
        except Exception:
            pass
    ready = qb.has_pending_message_for_task(task_id)
    pa.run_recovery()
    leaked = qa.has_pending_message_for_task(task_id)
    return (ready and not leaked), f"in-flight on q_b={ready} pushed to queue_messages={leaked}"


def _sqlite(app_filter: str | None) -> tuple[bool, str]:
    from stabilize import SqliteQueue, SqliteWorkflowStore

    with tempfile.TemporaryDirectory() as d:
        url = f"sqlite:///{d}/probe.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        qa = SqliteQueue(url, table_name="queue_messages")
        qb = SqliteQueue(url, table_name="q_b")
        qa._create_table()
        qb._create_table()
        try:
            return _case(store, qa, qb, app_filter)
        finally:
            qa.close()
            qb.close()
            store.close()


def _pg(dsn: str, app_filter: str | None) -> tuple[bool, str]:
    import psycopg

    from stabilize import PostgresQueue, PostgresWorkflowStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS q_b_dlq, q_b")
        conn.execute("DROP SEQUENCE IF EXISTS q_b_id_seq")
        conn.execute("CREATE TABLE q_b (LIKE queue_messages INCLUDING ALL)")
        conn.execute("CREATE TABLE q_b_dlq (LIKE queue_messages_dlq INCLUDING ALL)")
    store = PostgresWorkflowStore(dsn)
    qa = PostgresQueue(dsn, table_name="queue_messages")
    qb = PostgresQueue(dsn, table_name="q_b")
    qa.clear()
    qb.clear()
    try:
        return _case(store, qa, qb, app_filter)
    finally:
        qa.clear()
        qa.close()
        qb.close()
        store.close()


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    from _backends import postgres_dsn

    failures: list[str] = []
    with postgres_dsn() as dsn:
        runs: list[tuple[str, Any]] = []
        for label, flt in (("A unfiltered sweep", None), ("B CONTROL filtered sweep", "app-a")):
            runs.append((f"sqlite {label}", lambda f=flt: _sqlite(f)))
            if dsn is not None:
                runs.append((f"postgres {label}", lambda f=flt: _pg(dsn, f)))
        for label, fn in runs:
            ok, detail = fn()
            print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail} (want True/False)")
            if not ok:
                failures.append(label)
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — recovery does not duplicate work in flight on another table")
    return 0


if __name__ == "__main__":
    sys.exit(main())
