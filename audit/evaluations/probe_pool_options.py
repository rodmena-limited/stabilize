"""Audit probe: do caller-supplied pool options actually reach PostgreSQL,
does a shared pool survive one holder closing, and is is_healthy() bounded?

Tickets #18, #19, #22. Needs a reachable PostgreSQL; set STABILIZE_PROBE_DSN
or let the probe start a throwaway container itself.

Run:  python audit/evaluations/probe_pool_options.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid

CONTAINER = "stabilize-probe-pool"
PORT = 55433


def _start_container() -> str | None:
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
    started = subprocess.run(
        [
            "docker", "run", "-d", "--name", CONTAINER,
            "-e", "POSTGRES_PASSWORD=probepw", "-e", "POSTGRES_USER=probe",
            "-e", "POSTGRES_DB=probedb", "-p", f"{PORT}:5432", "postgres:16",
        ],
        capture_output=True, text=True, check=False,
    )
    if started.returncode != 0:
        return None
    for _ in range(40):
        ready = subprocess.run(
            ["docker", "exec", CONTAINER, "pg_isready", "-U", "probe", "-d", "probedb"],
            capture_output=True, check=False,
        )
        if ready.returncode == 0:
            return f"postgresql://probe:probepw@127.0.0.1:{PORT}/probedb"
        time.sleep(1)
    return None


def _show(pool, setting: str) -> str:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SHOW {setting}")
            row = cur.fetchone()
            return list(row.values())[0] if isinstance(row, dict) else row[0]


def main() -> int:
    from stabilize.persistence.connection import ConnectionManager, SingletonMeta
    from stabilize.persistence.pool_options import PoolOptions

    dsn = os.environ.get("STABILIZE_PROBE_DSN") or _start_container()
    owns_container = "STABILIZE_PROBE_DSN" not in os.environ
    if dsn is None:
        print("SKIP: no PostgreSQL available (set STABILIZE_PROBE_DSN or enable docker)")
        return 0

    failures = 0
    try:
        print("=" * 72)
        print("BASELINE first, so the check below can fail: no options supplied")
        SingletonMeta.reset(ConnectionManager)
        manager = ConnectionManager()
        pool = manager.get_postgres_pool(dsn, min_size=1, max_size=2)
        base_statement = _show(pool, "statement_timeout")
        base_lock = _show(pool, "lock_timeout")
        print(f"  statement_timeout : {base_statement}")
        print(f"  lock_timeout      : {base_lock}")
        if base_statement != "0":
            print("  >>> BASELINE UNEXPECTED: server already sets a statement_timeout;")
            print("      the passthrough check below cannot distinguish cause. Aborting.")
            return 1
        print("  >>> baseline green: unbounded, exactly as documented")
        SingletonMeta.reset(ConnectionManager)

        print()
        print("#18 — options supplied through PoolOptions.connect_kwargs")
        manager = ConnectionManager()
        pool = manager.get_postgres_pool(
            dsn,
            min_size=1,
            max_size=2,
            options=PoolOptions(
                connect_kwargs={"options": "-c statement_timeout=8000 -c lock_timeout=4000"}
            ),
        )
        got_statement = _show(pool, "statement_timeout")
        got_lock = _show(pool, "lock_timeout")
        print(f"  statement_timeout : {got_statement}")
        print(f"  lock_timeout      : {got_lock}")
        if got_statement == "8s" and got_lock == "4s":
            print("  >>> PASS: libpq honoured the caller's options")
        else:
            print("  >>> FAIL: options did not reach the server")
            failures += 1
        SingletonMeta.reset(ConnectionManager)

        print()
        print("#18 — configure callback runs per connection")
        marker = f"probe_{uuid.uuid4().hex[:8]}"

        def configure(conn) -> None:
            # psycopg_pool discards any connection left INTRANS by configure,
            # so the SET must be committed or run in autocommit.
            with conn.cursor() as cur:
                cur.execute(f"SET application_name = '{marker}'")
            conn.commit()

        manager = ConnectionManager()
        pool = manager.get_postgres_pool(
            dsn, min_size=1, max_size=2, options=PoolOptions(configure=configure)
        )
        seen = _show(pool, "application_name")
        print(f"  application_name  : {seen}")
        if seen == marker:
            print("  >>> PASS: configure callback ran")
        else:
            print("  >>> FAIL: configure callback did not run")
            failures += 1
        SingletonMeta.reset(ConnectionManager)

        print()
        print("#19 — one holder closing must not close the shared pool")
        manager = ConnectionManager()
        store_pool = manager.get_postgres_pool(dsn, min_size=1, max_size=2)
        queue_pool = manager.get_postgres_pool(dsn, min_size=1, max_size=2)
        print(f"  store and queue share one pool : {store_pool is queue_pool}")
        manager.close_postgres_pool(dsn)
        try:
            still_usable = _show(queue_pool, "statement_timeout") is not None
        except Exception as exc:
            still_usable = False
            print(f"  borrow after first close raised: {type(exc).__name__}")
        print(f"  queue pool usable after store.close(): {still_usable}")
        if still_usable:
            print("  >>> PASS")
        else:
            print("  >>> FAIL: the surviving holder lost its pool")
            failures += 1

        manager.close_postgres_pool(dsn)
        closed = getattr(queue_pool, "closed", None)
        print(f"  pool closed after last holder released : {closed}")
        if closed:
            print("  >>> PASS: both directions — it does eventually close")
        else:
            print("  >>> FAIL: pool never closes, so nothing is reclaimed")
            failures += 1
        SingletonMeta.reset(ConnectionManager)

        print()
        print("#22 — is_healthy() latency, healthy AND unhealthy")
        from stabilize.persistence.postgres.store import PostgresWorkflowStore

        manager = ConnectionManager()
        store = PostgresWorkflowStore(
            dsn, options=PoolOptions(min_size=1, max_size=2, acquire_timeout=2.0)
        )
        started = time.monotonic()
        healthy = store.is_healthy()
        healthy_elapsed = time.monotonic() - started
        print(f"  reachable   -> {healthy} in {healthy_elapsed:.3f}s")
        if not healthy:
            print("  >>> CONTROL FAILED: healthy path says False; latency result is vacuous")
            return 1
        SingletonMeta.reset(ConnectionManager)

        dead = PostgresWorkflowStore(
            "postgresql://u:p@127.0.0.1:1/nope",
            options=PoolOptions(min_size=0, max_size=1, acquire_timeout=1.0),
            health_timeout=1.0,
        )
        started = time.monotonic()
        unhealthy = dead.is_healthy()
        unhealthy_elapsed = time.monotonic() - started
        print(f"  unreachable -> {unhealthy} in {unhealthy_elapsed:.3f}s")
        if unhealthy is False and unhealthy_elapsed < 5.0:
            print("  >>> PASS: answers within a probe budget instead of ~30s")
        else:
            print(f"  >>> FAIL: took {unhealthy_elapsed:.1f}s or returned {unhealthy}")
            failures += 1
        SingletonMeta.reset(ConnectionManager)

    finally:
        if owns_container:
            subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
