"""The SQLite queue's clock is UTC to the millisecond on every SQLite and in every timezone.

0.31.0 compared stored UTC timestamps with datetime('now', 'utc'). On SQLite
3.40 and older (Debian bookworm's system SQLite, used by the official
python:3.11/3.12-slim-bookworm images) the 'utc' modifier treats 'now' as
local time, so the comparison moves by the host's UTC offset:
west of UTC a locked in-flight message is polled again at once (double
execution) and delays are ignored; east of UTC new messages are not delivered
for hours. datetime() also truncated to whole seconds, and the processed-mark
retention sweep compared "YYYY-MM-DD HH:MM:SS" with an ISO cutoff as text.

  A  an immediate message is delivered
  B  a message locked for 5 min is NOT polled again
  C  a message delayed 2 h is NOT delivered now
  D  a message delayed 700 ms is not delivered at 100 ms and is at 900 ms
  E  retention (1 h) keeps a mark written now (cutoff on the same calendar date)
  F  CONTROL: retention with max_age 0 h removes that same mark (E can say no)

Run under an old SQLite and a non-UTC zone to exercise the defect, e.g.
    docker run --rm -e TZ=America/New_York -v "$PWD":/src:ro python:3.11-slim-bookworm \\
      sh -c 'pip install -q /src && python /src/audit/evaluations/probe_sqlite_queue_clock.py'

    python audit/evaluations/probe_sqlite_queue_clock.py
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import tempfile
import time
from datetime import timedelta

logging.basicConfig(level=logging.CRITICAL)

from stabilize import SqliteQueue, SqliteWorkflowStore  # noqa: E402
from stabilize.queue.messages import StartWorkflow  # noqa: E402


def main() -> int:
    import stabilize

    print(f"stabilize {stabilize.__version__} sqlite {sqlite3.sqlite_version} TZ={os.environ.get('TZ', '(unset)')}")
    results: list[tuple[str, bool, str]] = []
    with tempfile.TemporaryDirectory() as d:
        url = f"sqlite:///{d}/clock.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        q = SqliteQueue(url, lock_duration=timedelta(minutes=5))
        q._create_table()

        q.push(StartWorkflow(execution_type="workflow", execution_id="now"))
        m = q.poll_one()
        results.append(("A immediate message delivered", m is not None, ""))
        again = q.poll_one() if m is not None else None
        results.append(("B locked message not re-polled", m is not None and again is None, ""))
        q.clear()

        q.push(StartWorkflow(execution_type="workflow", execution_id="later"), delay=timedelta(hours=2))
        results.append(("C 2 h delay respected", q.poll_one() is None, ""))
        q.clear()

        while time.time() % 1.0 > 0.1:
            time.sleep(0.01)
        q.push(StartWorkflow(execution_type="workflow", execution_id="soon"), delay=timedelta(milliseconds=700))
        early = q.poll_one()
        time.sleep(0.9)
        on_time = q.poll_one() if early is None else None
        results.append(("D 700 ms delay: not at 100 ms, yes at 900 ms", early is None and on_time is not None, ""))
        q.clear()

        store.mark_message_processed("mark-1", handler_type="StartStage", execution_id="w")
        store.cleanup_old_processed_messages(max_age_hours=1.0)
        kept = store.is_message_processed("mark-1")
        results.append(("E 1 h retention keeps a fresh mark", kept, ""))
        time.sleep(0.05)
        store.cleanup_old_processed_messages(max_age_hours=0.0)
        removed = not store.is_message_processed("mark-1")
        results.append(("F CONTROL 0 h retention removes it", removed, ""))
        q.close()
        store.close()

    failed = [name for name, ok, _ in results if not ok]
    for name, ok, _ in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
    if failed:
        print(f"VERDICT: FAIL — {', '.join(failed)}")
        return 1
    print("VERDICT: PASS — the SQLite queue clock is UTC and sub-second")
    return 0


if __name__ == "__main__":
    sys.exit(main())
