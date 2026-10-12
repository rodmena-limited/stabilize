"""A task outliving its queue lock is not executed a second time by another worker.

QueueProcessor renews the lock of an in-flight message with a heartbeat thread
so a handler running longer than the queue's lock_duration does not become
visible to other workers. This probe makes one heartbeat renewal raise (a
connection blip) and checks, through the queue API of a second worker PROCESS
polling the same table and through worker A's own polls, that the in-flight
RunTask is never delivered again while the task runs.

Worker A (this process, start()): lock_duration 1 s, heartbeat every 0.25 s,
runs a task that takes 6 s. Worker B (a subprocess, poll_one loop) starts once
A is inside the task and polls for 4.5 s, recording the type of every message
it receives; worker A's RunTask deliveries are counted as they are polled.
Executions are counted in a file each execution appends its pid to.

  A  one extend_lock call raises: RunTask redelivered 0 times, executions 1
  B  CONTROL: no fault: RunTask redelivered 0 times, executions 1

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


def worker(kind: str, url: str, seconds: float, seen_path: str) -> int:
    from stabilize.queue.messages import get_message_type_name

    store, queue = _open(kind, url, create=False)
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            msg = queue.poll_one()
        except Exception as exc:
            with open(seen_path, "a") as fh:
                fh.write(f"ERROR {type(exc).__name__}: {exc}\n".replace("\n", " ") + "\n")
            time.sleep(0.05)
            continue
        if msg is None:
            time.sleep(0.05)
            continue
        with open(seen_path, "a") as fh:
            fh.write(f"{get_message_type_name(msg)}\n")
    queue.close()
    store.close()
    return 0


def _backend_urls() -> Any:
    with tempfile.TemporaryDirectory() as d:
        yield "sqlite", f"sqlite:///{d}/probe.db"
    from _backends import postgres_dsn

    with postgres_dsn() as dsn:
        yield "postgres", dsn


def run_case(kind: str, url: str, inject: bool) -> tuple[int, int]:
    from stabilize import Orchestrator, QueueProcessor, StageExecution, TaskExecution, Workflow

    store, queue = _open(kind, url, create=True)
    queue.clear()
    marker = tempfile.NamedTemporaryFile(delete=False, suffix=".exec").name
    seen = tempfile.NamedTemporaryFile(delete=False, suffix=".seen").name
    proc = QueueProcessor(queue, store=store, task_registry=_registry())
    proc.config.retry_delay = timedelta(milliseconds=50)
    proc.config.lock_heartbeat_interval_seconds = 0.25
    from stabilize.queue.messages import get_message_type_name

    a_runtask_polls: list[float] = []
    poll_one = queue.poll_one

    def counting_poll_one() -> Any:
        msg = poll_one()
        if msg is not None and get_message_type_name(msg) == "RunTask":
            a_runtask_polls.append(time.monotonic())
        return msg

    queue.poll_one = counting_poll_one
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
        child = subprocess.Popen([sys.executable, __file__, "--worker", kind, url, str(TASK_SECONDS - 1.5), seen])
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
    seen_lines = [x for x in Path(seen).read_text().splitlines() if x]
    errors = [x for x in seen_lines if x.startswith("ERROR ")]
    if errors:
        print(f"[FAIL] {kind} PROBE INVALID: worker B could not poll: {errors[0][:200]}")
    stolen = [x for x in seen_lines if x == "RunTask"]
    os.unlink(marker)
    os.unlink(seen)
    redelivered = max(0, len(a_runtask_polls) - 1) + len(stolen)
    return len(lines), redelivered + (100 if errors else 0)


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        return worker(sys.argv[2], sys.argv[3], float(sys.argv[4]), sys.argv[5])
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for kind, url in _backend_urls():
        if url is None:
            print(f"[SKIP] {kind}: no PostgreSQL available")
            continue
        for label, inject in (("A one renewal raises", True), ("B CONTROL no fault", False)):
            executions, stolen = run_case(kind, url, inject)
            ok = executions == 1 and stolen == 0
            print(
                f"[{'PASS' if ok else 'FAIL'}] {kind} {label}: in-flight RunTask redelivered "
                f"{stolen} time(s) to worker A or B (want 0); executions={executions} (want 1)"
            )
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
