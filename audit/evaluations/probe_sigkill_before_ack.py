"""A processor killed (SIGKILL) after a handler commits and before the ack repeats no work.

For each of the 7 lifecycle message types, worker 1 (a subprocess) runs a
one-stage, one-task workflow and kills itself with SIGKILL in place of the
first ack of that type, so the handler's transaction is committed and the
queue row is still there, leased to a dead process. Worker 2 (another
subprocess) then runs the queue until the workflow completes. Every task
execution appends a line to a file.

  <type>          SIGKILL after commit, before ack: executions 1, SUCCEEDED
  CONTROL         no kill: executions 1, SUCCEEDED

The CONTROL proves the counting and the workers; a case where worker 1 was not
killed is reported as PROBE INVALID, not as a pass.

    AUDIT_ALLOW_DESTRUCTIVE=1 python audit/evaluations/probe_sigkill_before_ack.py
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

TYPES = ["StartWorkflow", "StartStage", "StartTask", "RunTask", "CompleteTask", "CompleteStage", "CompleteWorkflow"]
LOCK = timedelta(seconds=2)


def _open(kind: str, url: str, create: bool) -> tuple[Any, Any]:
    if kind == "sqlite":
        from stabilize import SqliteQueue, SqliteWorkflowStore

        store = SqliteWorkflowStore(url, create_tables=create)
        queue = SqliteQueue(url, table_name="queue_messages", lock_duration=LOCK)
        if create:
            queue._create_table()
        return store, queue
    from stabilize import PostgresQueue, PostgresWorkflowStore

    return PostgresWorkflowStore(url), PostgresQueue(url, table_name="queue_messages", lock_duration=LOCK)


def _processor(store: Any, queue: Any, marker: str) -> Any:
    from stabilize import QueueProcessor, StageExecution, TaskRegistry, TaskResult
    from stabilize.tasks.interface import Task

    class Effect(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            with open(marker, "a") as fh:
                fh.write(f"{os.getpid()}\n")
                fh.flush()
                os.fsync(fh.fileno())
            return TaskResult.success()

    registry = TaskRegistry()
    registry.register("effect", Effect)
    proc = QueueProcessor(queue, store=store, task_registry=registry)
    proc.config.retry_delay = timedelta(milliseconds=50)
    return proc


def worker(kind: str, url: str, marker: str, wf_id: str, kill_on: str, seconds: float) -> int:
    from stabilize.queue.messages import get_message_type_name

    store, queue = _open(kind, url, create=False)
    proc = _processor(store, queue, marker)
    if kill_on != "-":
        ack = queue.ack

        def ack_or_die(message: Any) -> None:
            if get_message_type_name(message) == kill_on:
                os.kill(os.getpid(), signal.SIGKILL)
            ack(message)

        queue.ack = ack_or_die
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if store.retrieve(wf_id).status.is_complete:
            break
        if not proc.process_one():
            time.sleep(0.05)
    queue.close()
    store.close()
    return 0


def run_case(kind: str, url: str, kill_on: str) -> tuple[int, str, int]:
    from stabilize import Orchestrator, StageExecution, TaskExecution, Workflow

    store, queue = _open(kind, url, create=True)
    queue.clear()
    marker = tempfile.NamedTemporaryFile(delete=False, suffix=".exec").name
    wf = Workflow.create(
        application="probe",
        name="sigkill",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("effect", "effect", stage_start=True, stage_end=True)],
            )
        ],
    )
    store.store(wf)
    Orchestrator(queue, store=store).start(wf)
    first = subprocess.run([sys.executable, __file__, "--worker", kind, url, marker, wf.id, kill_on, "30"], timeout=60)
    time.sleep(LOCK.total_seconds() + 0.5)
    subprocess.run([sys.executable, __file__, "--worker", kind, url, marker, wf.id, "-", "30"], timeout=60)
    status = store.retrieve(wf.id).status.name
    executions = len([x for x in Path(marker).read_text().splitlines() if x])
    os.unlink(marker)
    queue.close()
    store.close()
    return executions, status, first.returncode


def _backend_urls() -> Any:
    with tempfile.TemporaryDirectory() as d:
        yield "sqlite", f"sqlite:///{d}/probe.db"
    from _backends import postgres_dsn

    with postgres_dsn() as dsn:
        yield "postgres", dsn


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        return worker(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], float(sys.argv[7]))
    if os.environ.get("AUDIT_ALLOW_DESTRUCTIVE") != "1":
        print("[SKIP] kills its own worker processes; set AUDIT_ALLOW_DESTRUCTIVE=1")
        return 0
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for kind, url in _backend_urls():
        if url is None:
            print(f"[SKIP] {kind}: no PostgreSQL available")
            continue
        executions, status, rc = run_case(kind, url, "-")
        ok = executions == 1 and status == "SUCCEEDED" and rc == 0
        print(f"[{'PASS' if ok else 'FAIL'}] {kind} CONTROL no kill: executions={executions} status={status}")
        if not ok:
            failures.append(f"{kind}/CONTROL")
            continue
        for kill_on in TYPES:
            executions, status, rc = run_case(kind, url, kill_on)
            if rc != -signal.SIGKILL:
                print(f"[FAIL] {kind} {kill_on}: PROBE INVALID, worker 1 exited {rc}, not killed by SIGKILL")
                failures.append(f"{kind}/{kill_on}")
                continue
            ok = executions == 1 and status == "SUCCEEDED"
            print(
                f"[{'PASS' if ok else 'FAIL'}] {kind} {kill_on} SIGKILL after commit, before ack: "
                f"executions={executions} (want 1) status={status} (want SUCCEEDED)"
            )
            if not ok:
                failures.append(f"{kind}/{kill_on}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — a SIGKILL between commit and ack repeats no task and strands no workflow")
    return 0


if __name__ == "__main__":
    sys.exit(main())
