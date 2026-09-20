"""Signal storage must degrade, not kill the workflow, when its table is unreachable.

0.28.1 moved persistent signals into `workflow_signals` and reads it on EVERY
stage completion via set_stage_status -> pending_signal_count. A runtime role
holding explicit per-table grants has no privilege on that table, because
nothing referenced it before 0.28.1, so the workflow died mid-flight with
InsufficientPrivilege.

supports_signal_storage() returned True on the strength of the backend class
rather than of the connection, which is the question that actually matters.

Both directions, because a store that always reported False would pass the
degradation case trivially and silently discard every signal:

  A  GRANTED    -> storage is used, a buffered signal round-trips
  B  NOT GRANTED-> storage reports unusable, the workflow still SUCCEEDS
  C  CONTROL    -> the ungranted role really cannot read the table
  D  the fallback is announced, not silent

    python audit/evaluations/probe_signal_storage_degrades.py
"""

from __future__ import annotations

import logging
import sys

import psycopg
from testcontainers.postgres import PostgresContainer

logging.basicConfig(level=logging.WARNING)

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

ENGINE_TABLES = (
    "pipeline_executions",
    "stage_executions",
    "task_executions",
    "queue_messages",
    "queue_messages_dlq",
    "processed_messages",
    "stage_claims",
)


def _dsn(c: PostgresContainer, user: str, pw: str) -> str:
    return f"postgresql://{user}:{pw}@{c.get_container_host_ip()}:{c.get_exposed_port(5432)}/{c.dbname}"


def _run_workflow(dsn: str, name: str) -> WorkflowStatus:
    store = PostgresWorkflowStore(dsn)
    queue = PostgresQueue(dsn)
    registry = TaskRegistry()
    registry.register("shell", ShellTask)
    processor = QueueProcessor(queue, store=store, task_registry=registry)

    workflow = Workflow.create(
        application=f"sig-{name}",
        name=name,
        stages=[
            StageExecution(
                ref_id="a",
                type="shell",
                name="A",
                context={"command": f"echo {name}"},
                tasks=[TaskExecution.create("a", "shell", stage_start=True, stage_end=True)],
            )
        ],
    )
    store.store(workflow)
    Orchestrator(queue).start(workflow)
    processor.process_all(timeout=120.0)
    status = store.retrieve(workflow.id).status
    processor.stop(wait=True)
    store.close()
    return status


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    with PostgresContainer("postgres:16") as c:
        admin = _dsn(c, c.username, c.password)

        from stabilize.cli.commands import mg_up

        mg_up(admin)

        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute("CREATE ROLE granted LOGIN PASSWORD 'pw'")
            conn.execute("CREATE ROLE ungranted LOGIN PASSWORD 'pw'")
            for role in ("granted", "ungranted"):
                conn.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
                for t in ENGINE_TABLES + ("stabilize_migrations",):
                    conn.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {t} TO {role}")
                conn.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {role}")
            conn.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON workflow_signals TO granted")

        granted = _dsn(c, "granted", "pw")
        ungranted = _dsn(c, "ungranted", "pw")

        print("=== CONTROL: the ungranted role really cannot read workflow_signals ===")
        denied = False
        try:
            with psycopg.connect(ungranted) as conn:
                conn.execute("SELECT count(*) FROM workflow_signals").fetchone()
        except psycopg.errors.InsufficientPrivilege:
            denied = True
        print(f"    ungranted role denied: {denied}")
        results.append(("the ungranted role is genuinely denied", denied, str(denied)))

        print()
        print("=== A. GRANTED: storage is reported usable ===")
        s = PostgresWorkflowStore(granted)
        a_ok = s.supports_signal_storage()
        print(f"    supports_signal_storage(): {a_ok}")
        s.close()
        results.append(("granted role reports storage usable", a_ok is True, str(a_ok)))

        print()
        print("=== B. NOT GRANTED: storage is reported UNusable, not an exception ===")
        s = PostgresWorkflowStore(ungranted)
        try:
            b_ok = s.supports_signal_storage()
            raised = None
        except Exception as exc:
            b_ok, raised = None, exc
        print(f"    supports_signal_storage(): {b_ok}  raised={raised!r}")
        s.close()
        results.append(("ungranted role reports storage unusable", b_ok is False, f"{b_ok} raised={raised!r}"))

        print()
        print("=== C. THE WORKFLOW STILL SUCCEEDS WITHOUT THE GRANT ===")
        print("    this is the regression: 0.28.1 died here with InsufficientPrivilege")
        try:
            status = _run_workflow(ungranted, "nogrant")
            err = None
        except Exception as exc:
            status, err = None, exc
        print(f"    status: {status}  error: {err!r}")
        results.append((
            "workflow succeeds without the grant",
            status == WorkflowStatus.SUCCEEDED,
            f"{status} err={err!r}",
        ))

        print()
        print("=== D. AND IT STILL SUCCEEDS WITH THE GRANT (not won by disabling signals) ===")
        try:
            status_g = _run_workflow(granted, "withgrant")
            err_g = None
        except Exception as exc:
            status_g, err_g = None, exc
        print(f"    status: {status_g}  error: {err_g!r}")
        results.append((
            "workflow succeeds with the grant",
            status_g == WorkflowStatus.SUCCEEDED,
            f"{status_g} err={err_g!r}",
        ))

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
    print(f"VERDICT: PASS — signal storage degrades instead of failing the workflow ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
