"""PostgreSQL queue timing does not depend on a session's TimeZone.

0.31.0 kept deliver_at and locked_until as TIMESTAMP WITHOUT TIME ZONE,
written with NOW() (the writing session's wall clock) and compared with NOW()
(the reading session's wall clock). Two sessions with different TimeZone
settings -- PGTZ, a DSN options=-c timezone=..., a per-role default -- or one
non-UTC server crossing a DST change disagree about "now" by the offset:
messages stall for hours or delays and locks are cut short.

Sessions: one with TimeZone=America/New_York, one with TimeZone=UTC.

  A  a message pushed by the UTC session is delivered to the NY session now
  B  a 2 h delay pushed by the NY session is NOT delivered to the UTC session now
  C  a message the UTC session holds (60 s lock) is NOT polled by the NY session
  D  CONTROL: one session's own push/poll works (A-C are not a dead queue)
  E  a message the NY session holds (60 s lock) is NOT polled by the UTC session

    python audit/evaluations/probe_pg_queue_session_timezone.py
"""

from __future__ import annotations

import logging
import sys
from datetime import timedelta
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import postgres_dsn  # noqa: E402

from stabilize import PostgresQueue  # noqa: E402
from stabilize.queue.messages import StartWorkflow  # noqa: E402


def _with_tz(dsn: str, tz: str) -> str:
    sep = "&" if "?" in dsn else "?"
    return f"{dsn}{sep}options=-c%20timezone%3D{tz}"


def main() -> int:
    with postgres_dsn() as dsn:
        if dsn is None:
            print("[SKIP] no PostgreSQL available")
            return 0
        ny = PostgresQueue(_with_tz(dsn, "America/New_York"))
        utc = PostgresQueue(_with_tz(dsn, "UTC"))
        results = []
        try:
            utc.clear()
            utc.push(StartWorkflow(execution_type="workflow", execution_id="d"))
            results.append(("D CONTROL own push/poll", utc.poll_one() is not None))
            utc.clear()

            utc.push(StartWorkflow(execution_type="workflow", execution_id="a"))
            results.append(("A UTC push -> NY poll delivered", ny.poll_one() is not None))
            utc.clear()

            ny.push(StartWorkflow(execution_type="workflow", execution_id="b"), delay=timedelta(hours=2))
            results.append(("B NY 2 h delay not delivered to UTC now", utc.poll_one() is None))
            utc.clear()

            utc.push(StartWorkflow(execution_type="workflow", execution_id="c"))
            held = utc.poll_one()
            results.append(("C UTC-held message not polled by NY", held is not None and ny.poll_one() is None))
            utc.clear()

            ny.push(StartWorkflow(execution_type="workflow", execution_id="e"))
            held = ny.poll_one()
            results.append(("E NY-held message not polled by UTC", held is not None and utc.poll_one() is None))
            utc.clear()
        finally:
            ny.close()
            utc.close()
    failed = [n for n, ok in results if not ok]
    for n, ok in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {n}")
    if failed:
        print(f"VERDICT: FAIL — {', '.join(failed)}")
        return 1
    print("VERDICT: PASS — queue timing is absolute, not per-session wall time")
    return 0


if __name__ == "__main__":
    sys.exit(main())
