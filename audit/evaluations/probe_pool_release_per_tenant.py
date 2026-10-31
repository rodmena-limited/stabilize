"""Audit probe: after N tenants each open and close a queue and a store, does
PostgreSQL report zero backends for those tenants?

Ticket #52 (regression guard for #51). Observed at the server through
pg_stat_activity, not through ConnectionManager's own holder table. Needs a
reachable PostgreSQL; set STABILIZE_PROBE_DSN (a DSN with no query string) or
let the probe start a throwaway container itself.

Run:  python audit/evaluations/probe_pool_release_per_tenant.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _backends import probe_dsn  # noqa: E402

import os
import subprocess
import sys
import time

TENANTS = 4
OPS_PER_TENANT = 12
SETTLE_SECONDS = 10.0


def _tenant_backends(observer_dsn: str, prefix: str) -> int:
    import psycopg

    with psycopg.connect(observer_dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE %s",
            (prefix + "%",),
        ).fetchone()
        return int(row[0]) if row else -1


def _settled_count(observer_dsn: str, prefix: str) -> int:
    deadline = time.monotonic() + SETTLE_SECONDS
    count = _tenant_backends(observer_dsn, prefix)
    while count > 0 and time.monotonic() < deadline:
        time.sleep(0.5)
        count = _tenant_backends(observer_dsn, prefix)
    return count


def main() -> int:
    import stabilize
    from stabilize.cli.commands import mg_up
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue import PostgresQueue
    from stabilize.queue.messages import StartWorkflow

    print(f"stabilize {stabilize.__version__} from {stabilize.__file__}")
    base = probe_dsn("pooltenants")

    prefix = f"stabprobe{os.getpid()}_"
    failed = False
    try:
        mg_up(base)
        dsns = [f"{base}?application_name={prefix}{i}" for i in range(TENANTS)]

        owners = []
        for dsn in dsns:
            queue = PostgresQueue(dsn, table_name="queue_messages")
            store = PostgresWorkflowStore(dsn)
            for n in range(OPS_PER_TENANT):
                queue.push(StartWorkflow(execution_type="workflow", execution_id=f"e{n}"))
            queue.size()
            owners.append((queue, store))

        open_count = _tenant_backends(base, prefix)
        print(f"KNOWN-POSITIVE: tenant backends while {TENANTS} tenants are open: {open_count}")
        if open_count <= 0:
            print("  >>> FAIL: the observer cannot see tenant backends; a zero below would mean nothing")
            return 1

        for queue, store in owners:
            queue.clear()
            queue.close()
            store.close()

        after = _settled_count(base, prefix)
        print(f"tenant backends {SETTLE_SECONDS:.0f}s after every queue and store closed: {after}")
        if after == 0:
            print("  >>> PASS: every tenant's connections were released at the server")
        else:
            print(f"  >>> FAIL: {after} backend(s) still open for tenants whose owners all closed")
            failed = True
    finally:
        pass

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
