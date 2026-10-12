"""Two processor processes on one queue table execute every task exactly once.

The deployed shape is several worker processes polling the same queue table,
with default configuration (task lease off, heartbeat on). This runs two
processes, each with QueueProcessor.start() and 4 worker threads, over 20
workflows of 3 stages x 2 tasks (one stage polls RUNNING twice). Every task
execution appends "<task id> <pid>" to a shared file.

  A  every workflow SUCCEEDED within 90 s
  B  every task's completing execution happened exactly once, and every
     polling task polled exactly 3 times
  C  CONTROL both processes did work (each pid appears in the file)

    python audit/evaluations/probe_two_processes_execute_once.py
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

LOG = os.environ.get("PROBE_EXEC_LOG", "")


def _registry() -> Any:
    from stabilize import StageExecution, TaskRegistry, TaskResult
    from stabilize.tasks.interface import RetryableTask, Task

    def _log(stage: StageExecution, kind: str) -> None:
        task_id = stage.context.get("_task_marker", stage.id)
        with open(LOG, "a") as fh:
            fh.write(f"{kind} {stage.id}:{task_id} {os.getpid()}\n")

    class Work(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            _log(stage, f"work-{stage.context.get('slot', 0)}")
            stage.context["slot"] = int(stage.context.get("slot", 0)) + 1
            time.sleep(0.05)
            return TaskResult.success(context={"slot": stage.context["slot"]})

    class Poll(RetryableTask):
        def get_timeout(self) -> timedelta:
            return timedelta(minutes=5)

        def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
            return timedelta(milliseconds=100)

        def execute(self, stage: StageExecution) -> TaskResult:
            _log(stage, "poll")
            polls = int(stage.context.get("polls", 0)) + 1
            if polls < 3:
                return TaskResult.running(context={"polls": polls})
            return TaskResult.success(context={"polls": polls})

    r = TaskRegistry()
    r.register("work", Work)
    r.register("poll", Poll)
    return r


def _open(kind: str, url: str, create: bool) -> tuple[Any, Any]:
    if kind == "sqlite":
        from stabilize import SqliteQueue, SqliteWorkflowStore

        store = SqliteWorkflowStore(url, create_tables=create)
        queue = SqliteQueue(url, table_name="queue_messages")
        if create:
            queue._create_table()
        return store, queue
    from stabilize import PostgresQueue, PostgresWorkflowStore

    return PostgresWorkflowStore(url), PostgresQueue(url, table_name="queue_messages")


def _processor(store: Any, queue: Any) -> Any:
    from stabilize import QueueProcessor

    proc = QueueProcessor(queue, store=store, task_registry=_registry())
    proc.config.max_workers = 4
    proc.config.retry_delay = timedelta(milliseconds=100)
    return proc


def worker(kind: str, url: str, seconds: float) -> int:
    store, queue = _open(kind, url, create=False)
    proc = _processor(store, queue)
    proc.start()
    time.sleep(seconds)
    proc.stop(wait=True, timeout=20)
    return 0


def _workflows() -> list[Any]:
    from stabilize import StageExecution, TaskExecution, Workflow

    def st(ref: str, impls: list[str], needs: set[str]) -> StageExecution:
        n = len(impls)
        return StageExecution(
            ref_id=ref,
            type="probe",
            name=ref,
            requisite_stage_ref_ids=needs,
            tasks=[TaskExecution.create(f"{i}-{x}", x, stage_start=i == 0, stage_end=i == n - 1) for i, x in enumerate(impls)],
        )

    return [
        Workflow.create(
            application="probe",
            name=f"w{i}",
            stages=[st("a", ["work", "work"], set()), st("b", ["poll", "work"], {"a"}), st("c", ["work", "work"], {"a"})],
        )
        for i in range(20)
    ]


def run(kind: str, url: str) -> tuple[bool, bool, bool, str]:
    from stabilize import Orchestrator

    global LOG
    LOG = tempfile.NamedTemporaryFile(delete=False, suffix=".log").name
    os.environ["PROBE_EXEC_LOG"] = LOG
    store, queue = _open(kind, url, create=True)
    queue.clear()
    proc = _processor(store, queue)
    wfs = _workflows()
    for wf in wfs:
        store.store(wf)
    child = subprocess.Popen([sys.executable, __file__, "--worker", kind, url, "40"], env=dict(os.environ))
    proc.start()
    try:
        for wf in wfs:
            Orchestrator(queue, store=store).start(wf)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and not all(store.retrieve(w.id).status.is_complete for w in wfs):
            time.sleep(0.2)
    finally:
        proc.stop(wait=True, timeout=20)
        child.wait(timeout=120)
    statuses = Counter(store.retrieve(w.id).status.name for w in wfs)
    lines = [ln.split() for ln in Path(LOG).read_text().splitlines() if ln.strip()]
    os.unlink(LOG)
    per_key = Counter((kind_, key) for kind_, key, _ in lines)
    pids = {pid for _, _, pid in lines}
    work_dupes = sum(1 for (k, _), n in per_key.items() if k.startswith("work") and n != 1)
    poll_bad = sum(1 for (k, _), n in per_key.items() if k == "poll" and n != 3)
    a = statuses.get("SUCCEEDED", 0) == len(wfs)
    b = work_dupes == 0 and poll_bad == 0
    c = len(pids) == 2
    detail = f"statuses={dict(statuses)} work keys!=1: {work_dupes} poll keys!=3: {poll_bad} pids={len(pids)}"
    return a, b, c, detail


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        return worker(sys.argv[2], sys.argv[3], float(sys.argv[4]))
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    targets: list[tuple[str, str | None]] = []
    tmp = tempfile.TemporaryDirectory()
    targets.append(("sqlite", f"sqlite:///{tmp.name}/probe.db"))
    from _backends import postgres_dsn

    with postgres_dsn() as dsn:
        targets.append(("postgres", dsn))
        for kind, url in targets:
            if url is None:
                print(f"[SKIP] {kind}: no PostgreSQL available")
                continue
            a, b, c, detail = run(kind, url)
            for ok, label in ((a, "A all SUCCEEDED"), (b, "B each execution once"), (c, "C CONTROL both processes worked")):
                print(f"[{'PASS' if ok else 'FAIL'}] {kind} {label}: {detail}")
                if not ok:
                    failures.append(f"{kind}/{label}")
    tmp.cleanup()
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — two processes execute every task once")
    return 0


if __name__ == "__main__":
    sys.exit(main())
