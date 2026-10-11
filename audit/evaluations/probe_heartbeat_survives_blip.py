"""A task outliving its queue lock is not executed a second time by another worker.

QueueProcessor renews the lock of an in-flight message with a heartbeat thread
so a handler running longer than the queue's lock_duration does not become
visible to other workers. The task lease that would also stop a second
execution is off by default. This probe makes one heartbeat renewal raise (a
connection blip) and checks that a second worker PROCESS polling the same
table does not execute the still-running task.

Worker A (this process, start()): lock_duration 1 s, heartbeat every 0.25 s,
runs a task that takes 6 s. Worker B (a subprocess, process_one loop) starts
once A is inside the task and polls for 7 s. Executions are counted in a file
each execution appends its pid to.

  A  one extend_lock call raises: executions 1
  B  CONTROL: no fault: executions 1

    python audit/evaluations/probe_heartbeat_survives_blip.py
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

TASK_SECONDS = 6.0
LOCK = timedelta(seconds=1)


def _registry() -> Any:
    from stabilize import StageExecution, TaskRegistry, TaskResult
    from stabilize.tasks.interface import Task

    class Slow(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            with open(stage.context["marker"], "a") as fh:
                fh.write(f"{os.getpid()}\n")
            time.sleep(TASK_SECONDS)
            return TaskResult.success()

    registry = TaskRegistry()
    registry.register("slow", Slow)
    return registry


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


def worker(kind: str, url: str, seconds: float) -> int:
    from stabilize import QueueProcessor

    store, queue = _open(kind, url, create=False)
    proc = QueueProcessor(queue, store=store, task_registry=_registry())
    proc.config.retry_delay = timedelta(milliseconds=50)
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if not proc.process_one():
                time.sleep(0.05)
        except Exception:
            pass
    return 0


def _backend_urls() -> Any:
    with tempfile.TemporaryDirectory() as d:
        yield "sqlite", f"sqlite:///{d}/probe.db"
    from _backends import postgres_dsn

    with postgres_dsn() as dsn:
        yield "postgres", dsn


def run_case(kind: str, url: str, inject: bool) -> int:
    from stabilize import Orchestrator, QueueProcessor, StageExecution, TaskExecution, Workflow

    store, queue = _open(kind, url, create=True)
    queue.clear()
    marker = tempfile.NamedTemporaryFile(delete=False, suffix=".exec").name
    proc = QueueProcessor(queue, store=store, task_registry=_registry())
    proc.config.retry_delay = timedelta(milliseconds=50)
    proc.config.lock_heartbeat_interval_seconds = 0.25
    if inject:
        original = queue.extend_lock
        state = {"raised": False}

        def extend_lock(message: Any, duration: Any = None) -> bool:
            if not state["raised"]:
                state["raised"] = True
                raise ConnectionError("server closed the connection unexpectedly")
            return original(message, duration)

        queue.extend_lock = extend_lock
    wf = Workflow.create(
        application="probe",
        name="heartbeat",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                context={"marker": marker},
                tasks=[TaskExecution.create("slow", "slow", stage_start=True, stage_end=True)],
            )
        ],
    )
    store.store(wf)
    proc.start()
    child = None
    try:
        Orchestrator(queue, store=store).start(wf)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not Path(marker).read_text():
            time.sleep(0.02)
        child = subprocess.Popen([sys.executable, __file__, "--worker", kind, url, str(TASK_SECONDS + 1)])
        child.wait(timeout=30)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not store.retrieve(wf.id).status.is_complete:
            time.sleep(0.1)
    finally:
        if child is not None and child.poll() is None:
            child.kill()
        proc.stop(wait=True, timeout=10)
        queue.close()
        store.close()
    lines = [x for x in Path(marker).read_text().splitlines() if x]
    os.unlink(marker)
    return len(lines)


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        return worker(sys.argv[2], sys.argv[3], float(sys.argv[4]))
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for kind, url in _backend_urls():
        if url is None:
            print(f"[SKIP] {kind}: no PostgreSQL available")
            continue
        for label, inject in (("A one renewal raises", True), ("B CONTROL no fault", False)):
            executions = run_case(kind, url, inject)
            ok = executions == 1
            print(f"[{'PASS' if ok else 'FAIL'}] {kind} {label}: executions={executions} (want 1)")
            if not ok:
                failures.append(f"{kind}/{label}")
    a_failed = any("/A" in f for f in failures)
    b_failed = any("/B" in f for f in failures)
    print(f"[{'FAIL' if a_failed else 'PASS'}] any-backend A one renewal raises")
    print(f"[{'FAIL' if b_failed else 'PASS'}] all-backends B CONTROL no fault")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — a heartbeat blip does not let a second worker run the task")
    return 0


if __name__ == "__main__":
    sys.exit(main())
