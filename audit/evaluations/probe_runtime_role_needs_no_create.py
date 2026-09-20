"""Can the engine run a workflow with a role that has no CREATE on its schema?

A deployment that grants its runtime role CREATE only because the engine is
believed to need DDL is carrying a standing privilege that consents to any DDL
a future version decides to run. If the workflow store and queue perform no DDL,
that grant is removable without waiting for issue 39.

Measured in one run: migrations applied by an owner role, then a runtime role
holding only DML, then a real workflow driven to SUCCEEDED through the public
API.

    python audit/evaluations/probe_runtime_role_needs_no_create.py
"""

from __future__ import annotations

import sys

import psycopg
from testcontainers.postgres import PostgresContainer

from stabilize import (
    Orchestrator,
    PostgresQueue,
    PostgresWorkflowStore,
    QueueProcessor,
    ShellTask,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)

SCHEMA = "stabilize"
RUNTIME_ROLE = "ci_app_probe"
RUNTIME_PASSWORD = "dml-only"


def _dsn(container: PostgresContainer, user: str, password: str) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    return f"postgresql://{user}:{password}@{host}:{port}/{container.dbname}?schema={SCHEMA}"


def _apply_migrations(owner_dsn: str) -> list[str]:
    from stabilize.cli.commands import mg_up

    mg_up(owner_dsn)
    with psycopg.connect(owner_dsn.split("?", 1)[0]) as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s "
            "ORDER BY table_name",
            (SCHEMA,),
        ).fetchall()
    return [row[0] for row in rows]


def _make_runtime_role(owner_dsn: str) -> None:
    plain = owner_dsn.split("?", 1)[0]
    with psycopg.connect(plain, autocommit=True) as conn:
        conn.execute(f"CREATE ROLE {RUNTIME_ROLE} LOGIN PASSWORD '{RUNTIME_PASSWORD}'")
        conn.execute(f"GRANT CONNECT ON DATABASE {conn.info.dbname} TO {RUNTIME_ROLE}")
        conn.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {RUNTIME_ROLE}")
        conn.execute(
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {SCHEMA} "
            f"TO {RUNTIME_ROLE}"
        )
        conn.execute(
            f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {SCHEMA} TO {RUNTIME_ROLE}"
        )
        conn.execute(f"REVOKE CREATE ON SCHEMA {SCHEMA} FROM {RUNTIME_ROLE}")
        conn.execute(f"REVOKE CREATE ON SCHEMA {SCHEMA} FROM PUBLIC")
        conn.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")


def _privileges(runtime_dsn: str) -> dict[str, bool]:
    with psycopg.connect(runtime_dsn.split("?", 1)[0]) as conn:
        row = conn.execute(
            "SELECT current_user, "
            "has_schema_privilege(%s, 'CREATE'), "
            "has_schema_privilege('public', 'CREATE')",
            (SCHEMA,),
        ).fetchone()
    assert row is not None
    return {"user": row[0], f"create_on_{SCHEMA}": row[1], "create_on_public": row[2]}


def _run_workflow(runtime_dsn: str) -> str:
    plain = runtime_dsn.split("?", 1)[0]
    store = PostgresWorkflowStore(plain, schema=SCHEMA)
    queue = PostgresQueue(plain, schema=SCHEMA)
    registry = TaskRegistry()
    registry.register("shell", ShellTask)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    orchestrator = Orchestrator(queue)

    workflow = Workflow.create(
        application="privilege-probe",
        name="two-stage-no-create",
        stages=[
            StageExecution(
                ref_id="gate",
                type="shell",
                name="Gate",
                context={"command": "echo gate-ok"},
                tasks=[
                    TaskExecution.create(
                        name="gate",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    )
                ],
            ),
            StageExecution(
                ref_id="build",
                type="shell",
                name="Build",
                requisite_stage_ref_ids={"gate"},
                context={"command": "echo build-ok"},
                tasks=[
                    TaskExecution.create(
                        name="build",
                        implementing_class="shell",
                        stage_start=True,
                        stage_end=True,
                    )
                ],
            ),
        ],
    )
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=120.0)
    return str(store.retrieve(workflow.id).status)


def main() -> int:
    failures: list[str] = []

    with PostgresContainer("postgres:16") as container:
        owner_dsn = _dsn(container, container.username, container.password)

        print("=== 1. MIGRATIONS APPLIED BY AN OWNER ROLE (the pgadmin/migretti step) ===")
        tables = _apply_migrations(owner_dsn)
        print(f"    tables in schema {SCHEMA}: {tables}")
        if "stage_executions" not in tables:
            failures.append("migrations did not create stage_executions; probe cannot continue")
            print("    !! migrations failed, aborting")
            return 1

        print()
        print("=== 2. RUNTIME ROLE WITH NO CREATE ON THE SCHEMA ===")
        _make_runtime_role(owner_dsn)
        runtime_dsn = _dsn(container, RUNTIME_ROLE, RUNTIME_PASSWORD)
        print(f"    {_privileges(runtime_dsn)}")

        with psycopg.connect(runtime_dsn.split("?", 1)[0]) as conn:
            try:
                conn.execute(f"CREATE TABLE {SCHEMA}.probe_ddl_rights (x int)")
                failures.append("runtime role can CREATE; the whole probe proves nothing")
                print("    !! role can CREATE — restriction did not take")
            except psycopg.errors.InsufficientPrivilege:
                print("    control: role CANNOT create a table in the schema. Confirmed.")

        print()
        print("=== 3. A REAL TWO-STAGE WORKFLOW THROUGH THE PUBLIC API AS THAT ROLE ===")
        try:
            status = _run_workflow(runtime_dsn)
            print(f"    workflow status: {status}")
            if "SUCCEEDED" not in status:
                failures.append(f"workflow did not succeed: {status}")
        except Exception as exc:  # noqa: BLE001 - the observation is the exception
            failures.append(f"{type(exc).__name__}: {exc}")
            print(f"    RAISED {type(exc).__name__}: {exc}")

        print()
        print("=== 4. DID ANYTHING CREATE A TABLE BEHIND US? ===")
        with psycopg.connect(owner_dsn.split("?", 1)[0]) as conn:
            rows = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s "
                "ORDER BY table_name",
                (SCHEMA,),
            ).fetchall()
        after_tables = [row[0] for row in rows]
        new = sorted(set(after_tables) - set(tables))
        print(f"    tables now: {after_tables}")
        print(f"    created during the run: {new or 'none'}")
        if new:
            failures.append(f"runtime created tables: {new}")

    print()
    if failures:
        print("PROBE FAILED — the runtime role DOES need more than DML:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("PROBE CONCLUSIVE: the engine ran a workflow with NO CREATE on its schema.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
