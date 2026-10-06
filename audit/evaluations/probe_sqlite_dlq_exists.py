"""A SQLite deployment set up as QueueProcessor's docstring shows has a working DLQ.

SqliteWorkflowStore(create_tables=True) created queue_messages but not
queue_messages_dlq; only the private SqliteQueue._create_table() made it, and
every test fixture called that. A deployment following the QueueProcessor
docstring (store with create_tables=True, then SqliteQueue) had no DLQ table:
quarantining a poison message raised "no such table", the message stayed in
the queue, and list_dlq()/dlq_size() raised.

  A  a poison message is moved to the DLQ: dlq_size() == 1, queue empty
  B  a message that exhausts max_attempts is moved by check_and_move_expired
  C  CONTROL: with SqliteQueue._create_table() called, A passes (the probe can say yes)

    python audit/evaluations/probe_sqlite_dlq_exists.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
import time
from datetime import timedelta

logging.basicConfig(level=logging.CRITICAL)

from stabilize import QueueProcessor, SqliteQueue, SqliteWorkflowStore  # noqa: E402
from stabilize.queue.messages import StartWorkflow  # noqa: E402


def _poison(queue: SqliteQueue) -> None:
    conn = queue._get_connection()
    conn.execute(
        "INSERT INTO queue_messages (message_id, message_type, payload, deliver_at, attempts, max_attempts) "
        "VALUES ('poison-1', 'NoSuchMessageType', '{}', datetime('now', 'utc', '-1 second'), 0, 10)"
    )
    conn.commit()


def _case(create_queue_tables: bool) -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as d:
        url = f"sqlite:///{d}/app.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, lock_duration=timedelta(milliseconds=10), max_attempts=2)
        if create_queue_tables:
            queue._create_table()
        _poison(queue)
        processor = QueueProcessor(queue, store=store)
        try:
            processor.process_all(timeout=2.0)
        except Exception as e:
            return False, f"process_all raised {type(e).__name__}: {e}"
        try:
            dlq = queue.dlq_size()
        except Exception as e:
            return False, f"dlq_size raised {type(e).__name__}: {e}"
        queue.push(StartWorkflow(execution_type="workflow", execution_id="nobody"))
        for _ in range(3):
            queue.poll_one()
            time.sleep(1.1)
        try:
            moved = queue.check_and_move_expired()
        except Exception as e:
            return False, f"check_and_move_expired raised {type(e).__name__}: {e}"
        size = queue.size()
        ok = dlq == 1 and moved == 1 and size == 0
        return ok, f"poison->dlq_size={dlq}, expired moved={moved}, queue size={size}"


def main() -> int:
    failures = []
    for label, flag in (("C control (_create_table called)", True), ("A/B docstring setup", False)):
        ok, detail = _case(flag)
        print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")
        if not ok:
            failures.append(label)
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — the DLQ exists on the documented SQLite setup")
    return 0


if __name__ == "__main__":
    sys.exit(main())
