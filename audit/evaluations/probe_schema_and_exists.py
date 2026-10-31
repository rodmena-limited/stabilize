"""Audit probe (#24, reported by vellum-build-d8bbd2): a non-default schema.

Three claims:
  1. exists() must distinguish a missing TABLE from a missing ROW.
  2. The store and queue must find their tables via a schema= argument,
     without the caller hand-writing a search_path into the DSN.
  3. mg-status --db-url must honour MG_SCHEMA.

Needs STABILIZE_PROBE_DSN pointing at a gate database where the engine's
tables live in the schema named by STABILIZE_PROBE_SCHEMA.

Run:  python audit/evaluations/probe_schema_and_exists.py
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

SCHEMA = "orchestration"
ABSENT_ID = "01ABSENTWORKFLOWID000000000"


def main() -> int:
    from stabilize.persistence.postgres.store import PostgresWorkflowStore

    dsn = probe_dsn("schema")
    schema = os.environ.get("STABILIZE_PROBE_SCHEMA", SCHEMA)
    separator = "&" if "?" in dsn else "?"
    prepared = subprocess.run(
        [sys.executable, "-m", "stabilize.cli.main", "mg-up", "--db-url", f"{dsn}{separator}schema={schema}"],
        capture_output=True, text=True, check=False,
    )
    print(f"stabilize mg-up into schema {schema}: exit {prepared.returncode}")

    failures = 0
    try:
        print("=" * 72)
        print("CONTROL: the tables really are in a non-default schema")
        import psycopg

        with psycopg.connect(dsn) as conn:
            found = ",".join(
                r[0]
                for r in conn.execute(
                    "select schemaname from pg_tables where tablename='pipeline_executions' order by 1"
                ).fetchall()
            )
        print(f"  pipeline_executions lives in: {found!r}")
        if found != schema:
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
            missing_relation = "Applied 0 of" in text
            print(f"  {label:26} -> migration table unreachable: {missing_relation}")
            print(f"     MG_SCHEMA in env: {'MG_SCHEMA' in env}")
            print(f"     first line: {text.splitlines()[0][:70] if text.splitlines() else '(empty)'}")
            if missing_relation != expect_missing_relation:
                print("  >>> FAIL")
                failures += 1
            else:
                print("  >>> PASS")
    finally:
        pass

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
