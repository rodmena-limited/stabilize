"""Audit probe (#24, reported by vellum-build-d8bbd2): a non-default schema.

Three claims:
  1. exists() must distinguish a missing TABLE from a missing ROW.
  2. The store and queue must find their tables via a schema= argument,
     without the caller hand-writing a search_path into the DSN.
  3. mg-status --db-url must honour MG_SCHEMA.

Needs docker, or STABILIZE_PROBE_DSN pointing at a database where the engine's
tables live in the schema named by STABILIZE_PROBE_SCHEMA.

Run:  python audit/evaluations/probe_schema_and_exists.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

CONTAINER = "stabilize-probe-schema"
PORT = 55435
SCHEMA = "orchestration"
ABSENT_ID = "01ABSENTWORKFLOWID000000000"


def _start() -> str | None:
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
    if subprocess.run(
        ["docker", "run", "-d", "--name", CONTAINER,
         "-e", "POSTGRES_PASSWORD=pw", "-e", "POSTGRES_USER=vu", "-e", "POSTGRES_DB=vdb",
         "-p", f"{PORT}:5432", "postgres:16"],
        capture_output=True, check=False,
    ).returncode != 0:
        return None
    for _ in range(40):
        if subprocess.run(
            ["docker", "exec", CONTAINER, "pg_isready", "-U", "vu", "-d", "vdb"],
            capture_output=True, check=False,
        ).returncode == 0:
            dsn = f"postgresql://vu:pw@127.0.0.1:{PORT}/vdb"
            subprocess.run(
                ["docker", "exec", CONTAINER, "psql", "-U", "vu", "-d", "vdb",
                 "-c", f"CREATE SCHEMA {SCHEMA};"],
                capture_output=True, check=False,
            )
            env = {**os.environ, "MG_SCHEMA": SCHEMA, "MG_DATABASE_URL": dsn}
            subprocess.run([sys.executable, "-m", "stabilize.cli.main", "mg-up"],
                           env=env, capture_output=True, check=False)
            return dsn
        time.sleep(1)
    return None


def main() -> int:
    from stabilize.persistence.postgres.store import PostgresWorkflowStore

    dsn = os.environ.get("STABILIZE_PROBE_DSN") or _start()
    owns = "STABILIZE_PROBE_DSN" not in os.environ
    if dsn is None:
        print("SKIP: no PostgreSQL available (need docker or STABILIZE_PROBE_DSN)")
        return 0
    schema = os.environ.get("STABILIZE_PROBE_SCHEMA", SCHEMA)

    failures = 0
    try:
        print("=" * 72)
        print("CONTROL: the tables really are in a non-default schema")
        found = subprocess.run(
            ["docker", "exec", CONTAINER, "psql", "-U", "vu", "-d", "vdb", "-tAc",
             "select schemaname from pg_tables where tablename='pipeline_executions'"],
            capture_output=True, text=True, check=False,
        ).stdout.strip() if owns else schema
        print(f"  pipeline_executions lives in: {found!r}")
        if owns and found != schema:
            print("  >>> CONTROL FAILED: migrations did not land in the schema; probe invalid")
            return 1
        print("  >>> control green")

        print()
        print("CLAIM 1 — exists() distinguishes a missing TABLE from a missing ROW")
        blind = PostgresWorkflowStore(dsn)
        try:
            answer = blind.exists(ABSENT_ID)
            print(f"  no schema configured -> returned {answer} (did NOT raise)")
            print("  >>> FAIL: a broken deployment is reported as an empty one")
            failures += 1
        except Exception as exc:
            print(f"  no schema configured -> raises {type(exc).__name__}")
            print("  >>> PASS: the operational failure is visible")
        finally:
            blind.close()

        scoped = PostgresWorkflowStore(dsn, schema=schema)
        answer = scoped.exists(ABSENT_ID)
        print(f"  schema={schema!r}   -> returned {answer}")
        if answer is False:
            print("  >>> PASS: a genuinely absent workflow is still False")
        else:
            print("  >>> FAIL: absent workflow did not return False")
            failures += 1

        print()
        print("CLAIM 2 — schema= finds the tables with no hand-written DSN")
        try:
            scoped.retrieve_execution_summary(ABSENT_ID)
            print("  >>> FAIL: expected a not-found error")
            failures += 1
        except Exception as exc:
            name = type(exc).__name__
            print(f"  retrieve_execution_summary -> {name}")
            if "NotFound" in name:
                print("  >>> PASS: reached the table; the row is simply absent")
            else:
                print("  >>> FAIL: still cannot see the table")
                failures += 1
        scoped.close()

        print()
        print("CLAIM 3 — mg-status --db-url honours MG_SCHEMA")
        for label, env_schema, expect_applied in (
            ("MG_SCHEMA set", schema, True),
            ("MG_SCHEMA unset (control)", None, False),
        ):
            env = {k: v for k, v in os.environ.items() if k != "MG_SCHEMA"}
            if env_schema:
                env["MG_SCHEMA"] = env_schema
            out = subprocess.run(
                [sys.executable, "-m", "stabilize.cli.main", "mg-status", "--db-url", dsn],
                env=env, capture_output=True, text=True, check=False,
            )
            text = out.stdout + out.stderr
            applied = "\napplied " in text or text.startswith("applied ")
            print(f"  {label:26} -> applied rows: {applied}")
            print(f"     MG_SCHEMA in env: {'MG_SCHEMA' in env}")
            print(f"     first line: {text.splitlines()[0][:70] if text.splitlines() else '(empty)'}")
            if applied != expect_applied:
                print("  >>> FAIL")
                failures += 1
            else:
                print("  >>> PASS")
    finally:
        if owns:
            subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
