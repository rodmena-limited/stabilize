"""PostgresEventStore issues DDL from its constructor, by default.

A deployment that separates schema management from runtime — migrations applied
by an administrator, the application running with DML privileges only — cannot
construct the store. Both directions are measured in one run: an owner role,
which succeeds and is the known-positive that proves the probe can observe the
DDL at all, and a DML-only role, which is the condition such a deployment runs
under.

    python audit/evaluations/probe_event_store_ddl_on_construction.py
"""

from __future__ import annotations

import sys

import psycopg
from testcontainers.postgres import PostgresContainer

from stabilize.events.store.postgres.store import PostgresEventStore

DML_ROLE = "conductor_runtime"
DML_PASSWORD = "dml-only"


def _dsn(container: PostgresContainer, user: str, password: str) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    return f"postgresql://{user}:{password}@{host}:{port}/{container.dbname}"


def _make_dml_only_role(owner_dsn: str) -> None:
    with psycopg.connect(owner_dsn, autocommit=True) as conn:
        conn.execute(f"CREATE ROLE {DML_ROLE} LOGIN PASSWORD '{DML_PASSWORD}'")
        conn.execute(f"GRANT CONNECT ON DATABASE {conn.info.dbname} TO {DML_ROLE}")
        conn.execute(f"GRANT USAGE ON SCHEMA public TO {DML_ROLE}")
        conn.execute(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public "
            f"TO {DML_ROLE}"
        )
        conn.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {DML_ROLE}")
        conn.execute(f"REVOKE CREATE ON SCHEMA public FROM {DML_ROLE}")
        conn.execute(f"REVOKE CREATE ON SCHEMA public FROM PUBLIC")


def _columns_of_events(owner_dsn: str) -> set[str]:
    with psycopg.connect(owner_dsn) as conn:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'events'"
        ).fetchall()
    return {row[0] for row in rows}


def main() -> int:
    failures: list[str] = []

    with PostgresContainer("postgres:16") as container:
        owner_dsn = _dsn(container, container.username, container.password)

        print("=== A. OWNER ROLE — the known-positive control ===")
        print("    if this does not add commit_xid, the probe cannot observe DDL at all")
        before = _columns_of_events(owner_dsn)
        print(f"    events table exists before: {bool(before)}")

        PostgresEventStore(owner_dsn, create_tables=True)

        after = _columns_of_events(owner_dsn)
        added = sorted(after - before)
        print(f"    columns added by the constructor: {added}")
        if "commit_xid" not in after:
            failures.append("owner: commit_xid was not added, so the probe proves nothing")
        else:
            print("    commit_xid PRESENT -> the constructor performed DDL. Control passed.")

        print()
        print("=== B. DML-ONLY ROLE — the condition a consumer deploys under ===")
        _make_dml_only_role(owner_dsn)
        dml_dsn = _dsn(container, DML_ROLE, DML_PASSWORD)

        with psycopg.connect(dml_dsn) as conn:
            try:
                conn.execute("CREATE TABLE probe_ddl_rights (x int)")
                failures.append("dml role: has CREATE rights, so case B proves nothing")
                print("    !! role can CREATE — the restriction did not take")
            except psycopg.errors.InsufficientPrivilege:
                print("    control: role cannot CREATE TABLE. Restriction confirmed.")

        print("    constructing PostgresEventStore(dsn)  # create_tables defaults to True")
        try:
            PostgresEventStore(dml_dsn)
            print("    -> CONSTRUCTED WITHOUT ERROR")
            print("       (tables already existed and IF NOT EXISTS short-circuited)")
        except Exception as exc:  # noqa: BLE001 - the observation is the exception itself
            print(f"    -> RAISED {type(exc).__name__}: {exc}")

        print()
        print("=== C. DML-ONLY ROLE AGAINST A FRESH DATABASE ===")
        print("    the upgrade case: new tables or a new column the schema does not have yet")
        with psycopg.connect(owner_dsn, autocommit=True) as conn:
            conn.execute("ALTER TABLE events DROP COLUMN commit_xid")
            conn.execute("DROP INDEX IF EXISTS idx_events_commit")
        print("    dropped commit_xid to simulate a pre-0.27.0 schema")

        try:
            PostgresEventStore(dml_dsn)
            cols = _columns_of_events(owner_dsn)
            if "commit_xid" in cols:
                print("    -> CONSTRUCTED and commit_xid was ADDED by the DML-only role")
                failures.append("dml role added a column; the privilege model did not hold")
            else:
                print("    -> CONSTRUCTED WITHOUT ERROR and commit_xid was NOT added")
        except Exception as exc:  # noqa: BLE001
            print(f"    -> RAISED {type(exc).__name__}: {exc}")
            print("       A consumer on a DML-only role cannot construct the event store")
            print("       after an upgrade that adds a column.")

    print()
    if failures:
        print("PROBE INCONCLUSIVE:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("PROBE CONCLUSIVE: both directions observed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
