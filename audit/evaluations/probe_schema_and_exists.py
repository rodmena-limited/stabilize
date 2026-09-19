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
from pathlib import Path

CONTAINER = "stabilize-probe-schema"
PORT = 55435
SCHEMA = "orchestration"
ABSENT_ID = "01ABSENTWORKFLOWID000000000"


def _apply_repo_migrations() -> None:
    """Apply migrations/*.sql from the checkout into SCHEMA."""
    root = Path(__file__).resolve().parents[2] / "migrations"
    if not root.is_dir():
        return
    for path in sorted(root.glob("*.sql")):
        content = path.read_text()
        up = content.split("-- migrate: down")[0].replace("-- migrate: up", "")
        script = f"SET search_path TO {SCHEMA};\n{up}"
        subprocess.run(
            ["docker", "exec", "-i", CONTAINER, "psql", "-U", "vu", "-d", "vdb",
             "-v", "ON_ERROR_STOP=1", "-q"],
            input=script, text=True, capture_output=True, check=False,
        )


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
            applied = subprocess.run(
                [sys.executable, "-m", "stabilize.cli.main", "mg-up"],
                env=env, capture_output=True, text=True, check=False,
            )
            if "No migrations found in package" in (applied.stdout + applied.stderr):
                # Editable installs do not package stabilize.migrations. Apply
                # the repo's own SQL so the probe works from a checkout as well
                # as from a wheel.
                _apply_repo_migrations()
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
        # Assert on whether the query REACHED the schema, not on row content:
        # an editable checkout cannot enumerate packaged migrations, so it
        # prints an empty table even when the schema resolved correctly. The
        # fix under test is whether MG_SCHEMA is honoured, which the presence
        # or absence of the relation error states exactly.
        for label, env_schema, expect_missing_relation in (
            ("MG_SCHEMA set", schema, False),
            ("MG_SCHEMA unset (control)", None, True),
        ):
            env = {k: v for k, v in os.environ.items() if k != "MG_SCHEMA"}
            if env_schema:
                env["MG_SCHEMA"] = env_schema
            out = subprocess.run(
                [sys.executable, "-m", "stabilize.cli.main", "mg-status", "--db-url", dsn],
                env=env, capture_output=True, text=True, check=False,
            )
            text = out.stdout + out.stderr
            missing_relation = 'relation "stabilize_migrations" does not exist' in text
            print(f"  {label:26} -> migration table unreachable: {missing_relation}")
            print(f"     MG_SCHEMA in env: {'MG_SCHEMA' in env}")
            print(f"     first line: {text.splitlines()[0][:70] if text.splitlines() else '(empty)'}")
            if missing_relation != expect_missing_relation:
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
