"""Canceling a workflow runs the running task's on_cancel and on_cleanup hooks once.

Task.on_cancel is "called when the execution is canceled" and Task.on_cleanup
"for all terminal states: TERMINAL, CANCELED, ...". Resource release (a
container, a lease, a remote job) depends on them. A RetryableTask is polling
when Orchestrator.cancel is called.

  A  cancel during polling: workflow CANCELED, on_cancel called 1, on_cleanup called 1
  B  CONTROL the task succeeds without a cancel: on_cleanup called 1

    python audit/evaluations/probe_cancel_runs_task_hooks.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import timedelta
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import drive, processor, snapshot, stage, start, workflow  # noqa: E402

from stabilize import Orchestrator, StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.models.status import WorkflowStatus  # noqa: E402
from stabilize.tasks.interface import RetryableTask  # noqa: E402

CALLS = {"cancel": 0, "cleanup": 0, "polls": 0}


class Job(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=500)

    def execute(self, stage: StageExecution) -> TaskResult:
        CALLS["polls"] += 1
        if stage.context.get("finish"):
            return TaskResult.success()
        return TaskResult.running()

    def on_cancel(self, stage: StageExecution) -> TaskResult | None:
        CALLS["cancel"] += 1
        return None

    def on_cleanup(self, stage: StageExecution) -> None:
        CALLS["cleanup"] += 1


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, cancel in (("A cancel during polling", True), ("B CONTROL completes", False)):
            CALLS.update(cancel=0, cleanup=0, polls=0)
            registry = TaskRegistry()
            registry.register("job", Job)
            with factory() as (store, queue):
                proc = processor(store, queue, registry, task_backoff_min_delay_ms=500, task_backoff_max_delay_ms=500)
                wid = start(store, queue, workflow([stage("s", ["job"], {"finish": not cancel})]))
                deadline = time.monotonic() + 5
                while CALLS["polls"] == 0 and time.monotonic() < deadline:
                    try:
                        if not proc.process_one():
                            time.sleep(0.02)
                    except Exception:
                        pass
                if cancel:
                    Orchestrator(queue, store=store).cancel(store.retrieve(wid), "probe", "operator abort")
                wf = drive(proc, store, wid, timeout=10, idle=1.5)["workflow"]
            if cancel:
                ok = wf.status == WorkflowStatus.CANCELED and CALLS["cancel"] == 1 and CALLS["cleanup"] == 1
                want = "CANCELED, on_cancel 1, on_cleanup 1"
            else:
                ok = wf.status == WorkflowStatus.SUCCEEDED and CALLS["cleanup"] == 1
                want = "SUCCEEDED, on_cleanup 1"
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {snapshot(wf)} on_cancel={CALLS['cancel']} "
                f"on_cleanup={CALLS['cleanup']} polls={CALLS['polls']} (want {want})"
            )
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — cancel runs on_cancel and on_cleanup")
    return 0


if __name__ == "__main__":
    sys.exit(main())
