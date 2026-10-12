"""A RunTask re-issued by recovery runs the same task in the same bulkhead as before.

RunTask.task_type selects the task's bulkhead (TaskBulkheadManager names one per
task type). The normal path sets it to the task's implementing_class; recovery
of a RUNNING task whose RunTask was lost must do the same, and so must every
later re-queue copied from the recovered message.

A RetryableTask "job" (stage type "probe", bulkhead configured for "job") polls
once, its pending RunTask is removed with queue.purge_workflow (a lost message),
one recovery sweep runs, and the task polls again. The task records the thread
it ran on; bulkman names it after the bulkhead.

  A  the recovered poll runs in the "job" bulkhead
  B  CONTROL the first, normal poll runs in the "job" bulkhead

    python audit/evaluations/probe_recovered_runtask_type.py
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import stage, start, workflow  # noqa: E402

from stabilize import QueueProcessor, StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.resilience.bulkheads import TaskBulkheadManager  # noqa: E402
from stabilize.resilience.circuits import WorkflowCircuitFactory  # noqa: E402
from stabilize.resilience.config import BulkheadConfig, ResilienceConfig  # noqa: E402
from stabilize.tasks.interface import RetryableTask  # noqa: E402

THREADS: list[str] = []


class Job(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(minutes=10)

    def execute(self, stage: StageExecution) -> TaskResult:
        THREADS.append(threading.current_thread().name)
        return TaskResult.running()


def _until(proc: QueueProcessor, n: int, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while len(THREADS) < n and time.monotonic() < deadline:
        try:
            if not proc.process_one():
                time.sleep(0.02)
        except Exception:
            pass


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        THREADS.clear()
        rc = ResilienceConfig(bulkheads=(("job", BulkheadConfig(max_concurrent=2, max_queue_size=10)),), database_url=None)
        registry = TaskRegistry()
        registry.register("job", Job)
        with factory() as (store, queue):
            proc = QueueProcessor(
                queue,
                store=store,
                task_registry=registry,
                bulkhead_manager=TaskBulkheadManager(rc),
                circuit_factory=WorkflowCircuitFactory(rc),
            )
            proc.config.retry_delay = timedelta(milliseconds=50)
            proc.config.recovery_application = "probe"
            wid = start(store, queue, workflow([stage("s", ["job"])]))
            _until(proc, 1, 5)
            removed = queue.purge_workflow(wid)
            proc.run_recovery()
            _until(proc, 2, 5)
        first = THREADS[0] if THREADS else "<none>"
        second = THREADS[1] if len(THREADS) > 1 else "<none>"
        ok_b = "stabilize_job" in first
        ok_a = "stabilize_job" in second
        print(f"[{'PASS' if ok_a else 'FAIL'}] {name} A recovered poll bulkhead: thread={second!r} (want stabilize_job; lost messages={removed})")
        print(f"[{'PASS' if ok_b else 'FAIL'}] {name} B CONTROL normal poll bulkhead: thread={first!r} (want stabilize_job)")
        if not ok_a:
            failures.append(f"{name}/A")
        if not ok_b:
            failures.append(f"{name}/B")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — recovery keeps the task's bulkhead")
    return 0


if __name__ == "__main__":
    sys.exit(main())
