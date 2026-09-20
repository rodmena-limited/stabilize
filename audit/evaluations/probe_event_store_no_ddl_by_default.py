"""PostgresEventStore must not issue DDL unless asked, and must say what is missing.

A library that creates its own tables forces every consumer to grant its runtime
role a standing CREATE privilege — and that grant then consents to whatever DDL
a later release decides to run. The consumer cannot see what they are consenting
to, which makes it a privilege-boundary defect rather than a convenience.

Four directions in one run, because each alone is satisfiable by a broken store:
  A  default constructor issues NO DDL                (the fix)
  B  it REFUSES when the schema is absent, naming the DDL
  C  create_tables=True still works                   (escape hatch intact)
  D  against a prepared schema it constructs and WORKS (not merely refuses)

Without D a store that always refused would pass A and B.

    python audit/evaluations/probe_event_store_no_ddl_by_default.py
"""

from __future__ import annotations

import sys

import psycopg
from testcontainers.postgres import PostgresContainer

from stabilize.events.store.postgres import store as store_module
from stabilize.events.store.postgres.schema import EVENTS_COMMIT_XID_MIGRATION
from stabilize.events.store.postgres.store import PostgresEventStore


class _AbsentSchemaError(Exception):
    """Stand-in so this probe runs against a build that predates the real one."""


EventStoreSchemaError = getattr(store_module, "EventStoreSchemaError", _AbsentSchemaError)


def _dsn(container: PostgresContainer) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    return f"postgresql://{container.username}:{container.password}@{host}:{port}/{container.dbname}"


def _tables(dsn: str) -> set[str]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        ).fetchall()
    return {row[0] for row in rows}


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    with PostgresContainer("postgres:16") as container:
        dsn = _dsn(container)

        print("=== A. DEFAULT CONSTRUCTOR ISSUES NO DDL ===")
        before = _tables(dsn)
        try:
            PostgresEventStore(dsn)
            print("    constructed against an empty database (should not happen)")
        except EventStoreSchemaError:
            pass
        except Exception as exc:
            print(f"    unexpected {type(exc).__name__}: {exc}")
        after = _tables(dsn)
        created = sorted(after - before)
        print(f"    tables created by the default constructor: {created or 'none'}")
        results.append(("default constructor creates no tables", not created, str(created)))

        print()
        print("=== B. IT REFUSES, AND NAMES THE DDL ===")
        try:
            PostgresEventStore(dsn)
            results.append(("absent schema refuses", False, "constructed anyway"))
            results.append(("refusal carries copy-pasteable DDL", False, "no refusal"))
            print("    CONSTRUCTED ANYWAY — no refusal")
        except EventStoreSchemaError as exc:
            text = str(exc)
            names_tables = "events" in text and "event_subscriptions" in text
            has_ddl = "CREATE TABLE IF NOT EXISTS events" in text
            print(f"    RAISED EventStoreSchemaError; names the missing tables: {names_tables}")
            print(f"    carries runnable CREATE TABLE statements: {has_ddl}")
            results.append(("absent schema refuses", True, "EventStoreSchemaError"))
            results.append(("refusal carries copy-pasteable DDL", has_ddl and names_tables, text[:90]))
        except Exception as exc:
            print(f"    RAISED the wrong error: {type(exc).__name__}: {exc}")
            results.append(("absent schema refuses", False, type(exc).__name__))
            results.append(("refusal carries copy-pasteable DDL", False, type(exc).__name__))

        print()
        print("=== C. create_tables=True STILL WORKS (escape hatch intact) ===")
        PostgresEventStore(dsn, create_tables=True)
        made = _tables(dsn)
        ok_created = {"events", "snapshots", "event_subscriptions"} <= made
        print(f"    tables now present: {sorted(made)}")
        results.append(("explicit create_tables still creates", ok_created, str(sorted(made))))

        print()
        print("=== D. AGAINST A PREPARED SCHEMA IT CONSTRUCTS AND WORKS ===")
        print("    without this, a store that always refused would pass A and B")
        try:
            store = PostgresEventStore(dsn)
            supports = store.supports_commit_cursor()
            print(f"    constructed with create_tables=False; commit cursor available: {supports}")
            results.append(("prepared schema constructs", True, f"supports_commit_cursor={supports}"))
            results.append(("commit_xid detected without DDL", supports, str(supports)))
        except Exception as exc:
            print(f"    RAISED {type(exc).__name__}: {exc}")
            results.append(("prepared schema constructs", False, type(exc).__name__))
            results.append(("commit_xid detected without DDL", False, type(exc).__name__))

        print()
        print("=== E. AN OUT-OF-DATE SCHEMA IS DETECTED, NOT SILENTLY TOLERATED ===")
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("ALTER TABLE events DROP COLUMN commit_xid")
        try:
            PostgresEventStore(dsn)
            print("    CONSTRUCTED with a missing column")
            results.append(("stale schema refuses", False, "constructed with commit_xid absent"))
        except EventStoreSchemaError as exc:
            names_column = "events.commit_xid" in str(exc)
            print(f"    RAISED EventStoreSchemaError; names the column: {names_column}")
            results.append(("stale schema refuses", names_column, str(exc)[:90]))

        print()
        print("=== F. A TABLE IN THE WRONG SCHEMA DOES NOT SATISFY THE CHECK ===")
        print("    existence is not resolution: an information_schema lookup without")
        print("    a schema filter passes when the table exists ANYWHERE this role")
        print("    can see, while search_path resolves somewhere else entirely")
        with psycopg.connect(dsn, autocommit=True) as conn:
            # Case E dropped commit_xid and did not put it back. Without this
            # restore, F raises for a STALE-COLUMN reason on every version and
            # proves nothing about schema resolution -- it would pass on the
            # unfixed build too, which is how a check becomes vacuous.
            for statement in EVENTS_COMMIT_XID_MIGRATION:
                conn.execute(statement)
            conn.execute("CREATE SCHEMA IF NOT EXISTS decoy")
            conn.execute("ALTER TABLE events SET SCHEMA decoy")
        try:
            PostgresEventStore(dsn)
            print("    CONSTRUCTED although events resolves to nothing on this connection")
            results.append(("wrong-schema table is not accepted", False, "constructed anyway"))
        except EventStoreSchemaError as exc:
            print(f"    RAISED EventStoreSchemaError: {str(exc)[:70]}")
            results.append(("wrong-schema table is not accepted", True, "EventStoreSchemaError"))
        finally:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute("ALTER TABLE decoy.events SET SCHEMA public")

    print()
    failures = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1

    print()
    if failures:
        print(f"VERDICT: FAIL — {failures} of {len(results)} checks failed")
        return 1
    print(f"VERDICT: PASS — the store issues DDL only when asked ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
