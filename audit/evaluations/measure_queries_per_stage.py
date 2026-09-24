"""Measure: how many SQL statements does the engine issue to run N no-op stages, and from where?

Ticket #46. Every psycopg Cursor.execute/executemany is recorded with its SQL
and the innermost stabilize call site. Needs Docker, or STABILIZE_PROBE_DSN.

Run:  python audit/evaluations/measure_queries_per_stage.py [--detail]
"""

from __future__ import annotations

import collections
import os
import re
import subprocess
import sys
import time
import traceback
from typing import Any

CONTAINER = "stabilize-measure-queries"
PORT = 55435
RECORDS: list[tuple[str, str, str]] = []


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
            time.sleep(1)
            return f"postgresql://probe:probepw@127.0.0.1:{PORT}/probedb"
        time.sleep(1)
    return None


def _shape(sql: Any) -> str:
    text = sql.as_string(None) if hasattr(sql, "as_string") else str(sql)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:110]


def _site() -> tuple[str, str]:
    frames = traceback.extract_stack()[:-3]
    stab = [f for f in frames if "/stabilize/" in f.filename and "measure_queries" not in f.filename]
    if not stab:
        return "?", "?"
    inner = stab[-1]
    handler = next(
        (f for f in reversed(stab) if "/handlers/" in f.filename),
        next((f for f in reversed(stab) if "/queue/" in f.filename), stab[0]),
    )
    rel = lambda f: f"{f.filename.split('/stabilize/', 1)[1]}:{f.lineno} {f.name}"  # noqa: E731
    return rel(inner), rel(handler)


def _install_recorder() -> None:
    import psycopg

    real_execute = psycopg.Cursor.execute
    real_executemany = psycopg.Cursor.executemany

    def execute(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        inner, phase = _site()
        RECORDS.append((_shape(query), inner, phase))
        return real_execute(self, query, *args, **kwargs)

    def executemany(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        inner, phase = _site()
        RECORDS.append((_shape(query), inner, phase))
        return real_executemany(self, query, *args, **kwargs)

    psycopg.Cursor.execute = execute  # type: ignore[method-assign]
    psycopg.Cursor.executemany = executemany  # type: ignore[method-assign]


def _run(dsn: str, stages: int) -> int:
    from stabilize import Orchestrator, QueueProcessor, Task, TaskRegistry, TaskResult
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.models.workflow import Workflow
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue import PostgresQueue

    class NoOp(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            return TaskResult.success()

    store = PostgresWorkflowStore(dsn)
    queue = PostgresQueue(dsn, table_name="queue_messages")
    registry = TaskRegistry()
    registry.register("noop_task", NoOp)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    orchestrator = Orchestrator(queue)

    stage_list = []
    for i in range(stages):
        stage_list.append(
            StageExecution(
                ref_id=f"s{i}",
                type="noop_task",
                name=f"Stage {i}",
                requisite_stage_ref_ids=set() if i == 0 else {f"s{i - 1}"},
                tasks=[TaskExecution.create(name="t", implementing_class="noop_task", stage_start=True, stage_end=True)],
            )
        )
    workflow = Workflow.create(application="measure", name=f"n{stages}", stages=stage_list)

    RECORDS.clear()
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=30.0)
    count = len(RECORDS)
    status = store.retrieve(workflow.id).status
    queue.close()
    store.close()
    print(f"  {stages} stage(s): {count} statements, workflow {status.name}")
    return count


def main() -> int:
    detail = "--detail" in sys.argv
    base = os.environ.get("STABILIZE_PROBE_DSN") or _start_container()
    owns_container = "STABILIZE_PROBE_DSN" not in os.environ
    if base is None:
        print("SKIP: no PostgreSQL available")
        return 0
    try:
        from stabilize.cli.commands import mg_up

        mg_up(base)
        _install_recorder()

        import psycopg

        with psycopg.connect(base) as conn:
            RECORDS.clear()
            conn.execute("SELECT 1")
        print(f"CONTROL: one deliberate query recorded as {len(RECORDS)} statement(s)")
        if len(RECORDS) != 1:
            print("  >>> the recorder does not count what it should; results would mean nothing")
            return 1

        counts = {n: _run(base, n) for n in (1, 2, 4)}
        print(f"marginal statements per additional stage: {(counts[4] - counts[2]) / 2:.1f}")

        _run(base, 1)
        by_sql = collections.Counter(sql for sql, _i, _p in RECORDS)
        by_phase = collections.Counter(phase for _s, _i, phase in RECORDS)
        by_site = collections.Counter(inner for _s, inner, _p in RECORDS)
        print("\nONE STAGE, by statement:")
        for sql, n in by_sql.most_common():
            print(f"  {n:4d}  {sql}")
        print("\nONE STAGE, by handler / phase:")
        for phase, n in by_phase.most_common():
            print(f"  {n:4d}  {phase}")
        if detail:
            print("\nONE STAGE, by innermost call site:")
            for site, n in by_site.most_common():
                print(f"  {n:4d}  {site}")
    finally:
        if owns_container:
            subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
