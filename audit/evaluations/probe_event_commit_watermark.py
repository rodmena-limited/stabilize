"""Audit probe (#29): a durable subscription must not skip a slow commit.

``events.sequence`` is BIGSERIAL, assigned at INSERT and not at COMMIT. A reader
that advances its cursor to the highest sequence it can see will step over a
lower sequence belonging to a transaction that has not committed yet. When that
transaction does commit, its event is below the cursor and is never delivered.

  FINDING    an event inserted first but committed second is never delivered to
             a subscription that polled in between.

  CONTROL    with no concurrency, every event is delivered exactly once. Without
             this the probe could pass by breaking delivery altogether.

Needs a reachable PostgreSQL. Set STABILIZE_PROBE_DSN, or let the probe start a
throwaway container itself.

Run:  python audit/evaluations/probe_event_commit_watermark.py
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
import uuid

logging.basicConfig(level=logging.CRITICAL)

CONTAINER = "stabilize-probe-watermark"
PORT = 55434


def _start_container() -> str | None:
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
    started = subprocess.run(
        [
            "docker", "run", "-d", "--name", CONTAINER,
            "-e", "POSTGRES_PASSWORD=probe",
            "-e", "POSTGRES_DB=probe",
            "-p", f"{PORT}:5432",
            "postgres:16-alpine",
        ],
        capture_output=True, text=True, check=False,
    )
    if started.returncode != 0:
        print(f"SKIP: could not start container: {started.stderr.strip()}")
        return None
    dsn = f"postgresql://postgres:probe@127.0.0.1:{PORT}/probe"
    import psycopg

    for _ in range(60):
        try:
            with psycopg.connect(dsn, connect_timeout=2):
                return dsn
        except Exception:  # noqa: BLE001
            time.sleep(1)
    print("SKIP: container never became reachable")
    return None


def _stop_container() -> None:
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)


def main() -> int:
    import psycopg

    dsn = os.environ.get("STABILIZE_PROBE_DSN")
    owned = False
    if not dsn:
        dsn = _start_container()
        owned = True
        if not dsn:
            return 0  # environmental, not a defect

    try:
        from stabilize.events.base import EntityType, Event, EventType
        from stabilize.events.store.postgres import PostgresEventStore

        with psycopg.connect(dsn, autocommit=True) as setup:
            setup.execute("DROP TABLE IF EXISTS events CASCADE")
            setup.execute("DROP TABLE IF EXISTS event_subscriptions CASCADE")
            setup.execute("DROP TABLE IF EXISTS snapshots CASCADE")

        store = PostgresEventStore(dsn, create_tables=True)
        print(f"commit cursor available: {store.supports_commit_cursor()}")

        def _event(name: str) -> Event:
            return Event(
                event_type=EventType.WORKFLOW_STARTED,
                entity_type=EntityType.WORKFLOW,
                entity_id=name,
                workflow_id=name,
                data={"name": name},
            )

        slow_id = f"slow-{uuid.uuid4().hex[:6]}"
        fast_id = f"fast-{uuid.uuid4().hex[:6]}"

        # Slow writer inserts first and holds its transaction open.
        slow = psycopg.connect(dsn)
        store.append(_event(slow_id), connection=slow)

        # Fast writer inserts second and commits immediately.
        store.append(_event(fast_id))

        # Legacy sequence cursor, for contrast: it advances past the
        # uncommitted lower sequence and can never come back for it.
        legacy_seen: list[str] = []
        legacy_cursor = 0
        for event in store.get_events_since(legacy_cursor, limit=100):
            legacy_seen.append(event.workflow_id)
            legacy_cursor = max(legacy_cursor, event.sequence)

        delivered: list[str] = []
        cursor = "0"

        events, cursor = store.get_events_since_committed(cursor, limit=100)
        delivered += [e.workflow_id for e in events]
        print(f"poll #1 delivers {[e.workflow_id for e in events]}")

        slow.commit()
        slow.close()

        events, cursor = store.get_events_since_committed(cursor, limit=100)
        delivered += [e.workflow_id for e in events]
        print(f"poll #2 delivers {[e.workflow_id for e in events]}")

        events, cursor = store.get_events_since_committed(cursor, limit=100)
        delivered += [e.workflow_id for e in events]
        print(f"poll #3 delivers {[e.workflow_id for e in events]}")

        legacy_after = list(legacy_seen)
        for event in store.get_events_since(legacy_cursor, limit=100):
            legacy_after.append(event.workflow_id)

        print(f"delivered (commit cursor):   {delivered}")
        print(f"delivered (sequence cursor): {legacy_after}")
        if slow_id not in legacy_after:
            print(f"  -> the sequence cursor lost {slow_id}: this is the defect being fixed")
        print()

        store.close()

        if fast_id not in delivered:
            print("FAIL (control): a committed event was never delivered at all.")
            return 1
        if len(delivered) != len(set(delivered)):
            print(f"FAIL (#29): an event was delivered more than once: {delivered}")
            return 1
        if slow_id not in delivered:
            print("FAIL (#29): the slow-committing event was skipped and is undeliverable.")
            return 1

        print("PASS: no event is skipped or duplicated by a concurrent commit.")
        return 0
    finally:
        if owned:
            _stop_container()


if __name__ == "__main__":
    sys.exit(main())
