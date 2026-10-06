"""The PostgreSQL queue keeps accepting messages past 2^31 pushes.

queue_messages.id was SERIAL (int4). Every push consumes one sequence value --
including each re-queue of a polling RUNNING task -- so after 2,147,483,647
pushes over the database's life every push raised
SequenceGeneratorLimitExceeded and the engine stopped. The DLQ's id and
original_id were int4 as well.

  A  with the queue sequence set to 2147483645, three pushes succeed, the
     message above 2^31 polls, acks, and dead-letters with its original_id
  B  CONTROL: the column types read back as bigint (so A is not passing on a
     sequence that was never near its limit)

SAFETY: this moves a sequence. It runs only in its own throwaway
testcontainers PostgreSQL, unless AUDIT_ALLOW_DESTRUCTIVE=1 and
STABILIZE_PROBE_DSN point it at a database you are willing to alter.

    python audit/evaluations/probe_queue_id_capacity.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import _container_dsn, dedicated_dsn  # noqa: E402


def main() -> int:
    import psycopg

    from stabilize import PostgresQueue
    from stabilize.cli.commands import mg_up
    from stabilize.queue.messages import StartWorkflow

    container = None
    if os.environ.get("AUDIT_ALLOW_DESTRUCTIVE") == "1" and os.environ.get("STABILIZE_PROBE_DSN"):
        dsn = dedicated_dsn(os.environ["STABILIZE_PROBE_DSN"], "capacity")
        print(f"BLAST RADIUS: moving queue_messages_id_seq on {dsn.split('@')[-1]}")
    else:
        dsn, container = _container_dsn()
        if dsn is None:
            print("[SKIP] no throwaway PostgreSQL available (docker/testcontainers)")
            return 0
    try:
        mg_up(dsn)
        with psycopg.connect(dsn, autocommit=True) as c:
            types = dict(
                c.execute(
                    "SELECT table_name || '.' || column_name, data_type FROM information_schema.columns "
                    "WHERE table_name IN ('queue_messages', 'queue_messages_dlq') "
                    "AND column_name IN ('id', 'original_id')"
                ).fetchall()
            )
            c.execute("SELECT setval('queue_messages_id_seq', 2147483645)")
        b_ok = set(types.values()) == {"bigint"} and len(types) == 3
        print(f"[{'PASS' if b_ok else 'FAIL'}] B column types: {types}")

        q = PostgresQueue(dsn)
        q.clear()
        error = None
        try:
            for i in range(3):
                q.push(StartWorkflow(execution_type="workflow", execution_id=f"e{i}"))
            first = q.poll_one()
            q.ack(first)
            q.ack(q.poll_one())
            second = q.poll_one()
            q.move_to_dlq(second.message_id, error="probe")
            dlq = q.list_dlq(limit=1)[0]
        except Exception as e:
            error = f"{type(e).__name__}: {str(e).splitlines()[0]}"
        finally:
            q.clear()
            q.clear_dlq()
            q.close()
        a_ok = error is None and int(second.message_id) > 2**31 - 1
        detail = error or f"polled {first.message_id}, {second.message_id}; dlq original_id={dlq.get('original_id')}"
        print(f"[{'PASS' if a_ok else 'FAIL'}] A pushes past 2^31: {detail}")
    finally:
        if container is not None:
            container.stop()
    if a_ok and b_ok:
        print("VERDICT: PASS — the queue id space is 64-bit")
        return 0
    print("VERDICT: FAIL")
    return 1


if __name__ == "__main__":
    sys.exit(main())
