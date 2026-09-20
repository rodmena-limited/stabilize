"""The engine must keep working under RESTRICTIVE row-level security.

A downstream platform embedding stabilize in a multi-tenant database cannot
scope the engine's tables through a caller-side helper, because the engine
constructs its own connections from a raw DSN. The claim is that it works
anyway: pools are keyed by the exact DSN string, so a per-tenant DSN gets its
own pool; a libpq `options` parameter sets a custom GUC at connect; INSERTs use
explicit column lists so a tenant_id column with DEFAULT current_setting() fills
itself; and every converter indexes rows by name, so an added column is
invisible to it.

That claim was given to a consumer as a yes. This probe is what turns it into a
check — the ticket's own words: the value here is the test, not new features.

Both directions, because an engine that could not see ANY row would satisfy
isolation trivially:

  A  tenant A runs a workflow to SUCCEEDED under RESTRICTIVE RLS
  B  tenant B runs one too, independently
  C  ISOLATION: neither tenant's connection can see the other's rows
  D  the rows really are there when RLS is bypassed (the leak-check control)
  E  FOR UPDATE SKIP LOCKED queue polling works under the policy, not just SELECT

    python audit/evaluations/probe_multitenant_rls.py
"""

from __future__ import annotations

import logging
import sys
from urllib.parse import quote

import psycopg
from testcontainers.postgres import PostgresContainer

logging.basicConfig(level=logging.CRITICAL)

from stabilize import (  # noqa: E402
    Orchestrator,
    PostgresQueue,
    PostgresWorkflowStore,
    QueueProcessor,
    ShellTask,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStatus,
)

TENANT_TABLES = (
    "pipeline_executions",
    "stage_executions",
    "task_executions",
    "queue_messages",
    "queue_messages_dlq",
    "processed_messages",
    "stage_claims",
)

APP_ROLE = "stabilize_app"
APP_PASSWORD = "app-pw"


def _admin_dsn(container: PostgresContainer) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    return f"postgresql://{container.username}:{container.password}@{host}:{port}/{container.dbname}"


def _tenant_dsn(container: PostgresContainer, tenant: str) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    opts = quote(f"-c app.tenant_id={tenant}")
    return (
        f"postgresql://{APP_ROLE}:{APP_PASSWORD}@{host}:{port}/{container.dbname}"
        f"?options={opts}"
    )


def _apply_migrations(admin_dsn: str) -> None:
    """Apply the shipped migrations through the CLI's own resolver."""
    from stabilize.cli.migrations import extract_up_migration, get_migrations

    sql_files = get_migrations()

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        for name, content in sql_files:
            conn.execute(extract_up_migration(content))
    print(f"    applied {len(sql_files)} migration(s)")


def _install_rls(admin_dsn: str) -> None:
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(f"CREATE ROLE {APP_ROLE} LOGIN PASSWORD '{APP_PASSWORD}'")
        conn.execute(f"GRANT USAGE ON SCHEMA public TO {APP_ROLE}")
        for table in TENANT_TABLES:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS tenant_id text "
                "NOT NULL DEFAULT current_setting('app.tenant_id', true)"
            )
            conn.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
            conn.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
            # PostgreSQL denies by default and RESTRICTIVE policies only
            # SUBTRACT from permissive ones. A purely restrictive policy set
            # therefore denies everything, including the tenant's own rows, so
            # a real deployment needs a permissive grant AND a restrictive
            # tenant constraint. Getting this wrong looks exactly like the
            # engine being incompatible with RLS.
            conn.execute(f"CREATE POLICY tenant_access ON {table} FOR ALL USING (true) WITH CHECK (true)")
            conn.execute(
                f"CREATE POLICY tenant_isolation ON {table} AS RESTRICTIVE "
                "USING (tenant_id = current_setting('app.tenant_id', true)) "
                "WITH CHECK (tenant_id = current_setting('app.tenant_id', true))"
            )
            conn.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {APP_ROLE}"
            )
        conn.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {APP_ROLE}")


def _run_workflow(dsn: str, name: str) -> WorkflowStatus:
    store = PostgresWorkflowStore(dsn)
    queue = PostgresQueue(dsn)
    registry = TaskRegistry()
    registry.register("shell", ShellTask)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    orchestrator = Orchestrator(queue)

    workflow = Workflow.create(
        application=f"rls-{name}",
        name=name,
        stages=[
            StageExecution(
                ref_id="a",
                type="shell",
                name="A",
                context={"command": f"echo {name}-a"},
                tasks=[TaskExecution.create("a", "shell", stage_start=True, stage_end=True)],
            ),
            StageExecution(
                ref_id="b",
                type="shell",
                name="B",
                requisite_stage_ref_ids={"a"},
                context={"command": f"echo {name}-b"},
                tasks=[TaskExecution.create("b", "shell", stage_start=True, stage_end=True)],
            ),
        ],
    )
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=120.0)
    status = store.retrieve(workflow.id).status
    processor.stop(wait=True)
    store.close()
    return status


def _visible_rows(dsn: str, table: str) -> int:
    with psycopg.connect(dsn) as conn:
        return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    with PostgresContainer("postgres:16") as container:
        admin = _admin_dsn(container)

        print("=== SETUP: migrate as owner, then install RESTRICTIVE RLS ===")
        _apply_migrations(admin)
        _install_rls(admin)
        print(f"    {len(TENANT_TABLES)} tables carry tenant_id + RESTRICTIVE policy")

        dsn_a = _tenant_dsn(container, "tenant-a")
        dsn_b = _tenant_dsn(container, "tenant-b")

        print()
        print("=== CONTROL: the GUC actually arrives through the DSN ===")
        print("    if it did not, every policy would deny and A would fail for the wrong reason")
        with psycopg.connect(dsn_a) as conn:
            guc = conn.execute("SELECT current_setting('app.tenant_id', true)").fetchone()[0]
        print(f"    current_setting('app.tenant_id') = {guc!r}")
        results.append(("the tenant GUC reaches the session", guc == "tenant-a", repr(guc)))

        print()
        print("=== A. TENANT A RUNS A WORKFLOW UNDER RLS ===")
        status_a = _run_workflow(dsn_a, "alpha")
        print(f"    workflow status: {status_a}")
        results.append(("tenant A workflow succeeds", status_a == WorkflowStatus.SUCCEEDED, str(status_a)))

        print()
        print("=== B. TENANT B RUNS ONE INDEPENDENTLY ===")
        status_b = _run_workflow(dsn_b, "beta")
        print(f"    workflow status: {status_b}")
        results.append(("tenant B workflow succeeds", status_b == WorkflowStatus.SUCCEEDED, str(status_b)))

        print()
        print("=== C. ISOLATION: neither tenant sees the other's rows ===")
        a_sees = _visible_rows(dsn_a, "pipeline_executions")
        b_sees = _visible_rows(dsn_b, "pipeline_executions")
        admin_sees = _visible_rows(admin, "pipeline_executions")
        print(f"    tenant A sees {a_sees}; tenant B sees {b_sees}; owner (RLS forced) sees {admin_sees}")
        results.append(("tenant A sees exactly its own", a_sees == 1, f"{a_sees} rows"))
        results.append(("tenant B sees exactly its own", b_sees == 1, f"{b_sees} rows"))

        print()
        print("=== D. LEAK-CHECK CONTROL: both rows really exist ===")
        print("    an engine that wrote nothing would pass C trivially")
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute("ALTER TABLE pipeline_executions NO FORCE ROW LEVEL SECURITY")
            total = conn.execute("SELECT count(*) FROM pipeline_executions").fetchone()[0]
            tenants = [
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT tenant_id FROM pipeline_executions ORDER BY 1"
                ).fetchall()
            ]
        print(f"    rows with RLS bypassed: {total}; distinct tenant_id: {tenants}")
        results.append(("both tenants' rows were really written", total == 2, f"{total} rows"))
        results.append(
            ("tenant_id defaulted from the GUC", tenants == ["tenant-a", "tenant-b"], str(tenants))
        )

        print()
        print("=== E. FOR UPDATE SKIP LOCKED WORKS UNDER THE POLICY ===")
        print("    queue polling uses it; a policy admitting plain SELECT may not admit this")
        with psycopg.connect(dsn_a) as conn:
            rows = conn.execute(
                "SELECT id FROM queue_messages ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 5"
            ).fetchall()
        print(f"    locked-read returned {len(rows)} row(s) without error")
        results.append(("FOR UPDATE SKIP LOCKED is permitted", True, f"{len(rows)} rows"))

    print()
    failures = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1

    print()
    print("NOT COVERED, stated rather than implied: crash-recovery sweeps run from")
    print("whichever processor is live, so under RLS a tenant's orphans are only")
    print("recoverable while something runs for that tenant. Connection count also")
    print("scales at one pool per tenant DSN. Both are deployment properties this")
    print("probe does not exercise.")

    print()
    if failures:
        print(f"VERDICT: FAIL — {failures} of {len(results)} checks failed")
        return 1
    print(f"VERDICT: PASS — the engine operates correctly under RESTRICTIVE RLS ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
