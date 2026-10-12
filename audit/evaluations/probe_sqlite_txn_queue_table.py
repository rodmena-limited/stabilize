"""A message pushed inside a SQLite store transaction lands on the queue's own table.

Handlers push their next message with store.transaction(queue).push_message(),
so a processor whose SqliteQueue uses table_name="q_b" only ever sees messages
that this push writes to q_b. PostgreSQL's transaction pushes through the
queue object; SQLite's must use the same table.

  A  SqliteQueue(table_name="q_b"): a workflow started on it runs to SUCCEEDED
     with a processor polling only q_b, and queue_messages stays empty
  B  CONTROL SqliteQueue(table_name="queue_messages"): SUCCEEDED

    python audit/evaluations/probe_sqlite_txn_queue_table.py
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _engine import drive, processor, snapshot, stage, start, workflow  # noqa: E402

from stabilize import SqliteQueue, SqliteWorkflowStore, StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.tasks.interface import Task  # noqa: E402


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for label, table in (("A table q_b", "q_b"), ("B CONTROL table queue_messages", "queue_messages")):
        with tempfile.TemporaryDirectory() as d:
            url = f"sqlite:///{d}/probe.db"
            store = SqliteWorkflowStore(url, create_tables=True)
            queue = SqliteQueue(url, table_name=table)
            queue._create_table()
            other = SqliteQueue(url, table_name="queue_messages")
            registry = TaskRegistry()
            registry.register("ok", Ok)
            try:
                proc = processor(store, queue, registry)
                wid = start(store, queue, workflow([stage("s", ["ok", "ok"])]))
                wf = drive(proc, store, wid, timeout=5, idle=0.5)["workflow"]
                stray = other.size() if table != "queue_messages" else 0
            finally:
                queue.close()
                other.close()
                store.close()
        ok = wf.status.name == "SUCCEEDED" and stray == 0
        print(f"[{'PASS' if ok else 'FAIL'}] sqlite {label}: {snapshot(wf)} stray messages on queue_messages={stray} (want SUCCEEDED, 0)")
        if not ok:
            failures.append(label)
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — SQLite transactional pushes honour the queue's table")
    return 0


if __name__ == "__main__":
    sys.exit(main())
