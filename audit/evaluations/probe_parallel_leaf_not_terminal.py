"""A workflow is not failed while one of its branches is still running.

When a leaf stage completes, CompleteWorkflow checks the other top-level stages.
If some are still running it re-queues itself, and after
max_stage_wait_retries it marks the workflow TERMINAL and cancels what is
left. A branch that legitimately runs longer than
max_stage_wait_retries x handler_retry_delay_seconds (240 x 15 s = 1 h by
default) is therefore failed while healthy.

Two parallel stages: "fast" finishes at once; "slow" polls (RUNNING) for 3 s.
STABILIZE_MAX_STAGE_WAIT_RETRIES=3 and STABILIZE_HANDLER_RETRY_DELAY_S=0.2, so the
budget is 0.6 s. (The environment, not QueueProcessor(handler_config=...), is what
CompleteWorkflowHandler reads; see probe_handler_config_reaches_handlers.py.)

  A  budget 0.6 s: workflow SUCCEEDED, slow SUCCEEDED
  B  CONTROL budget 100 x 0.2 s: workflow SUCCEEDED, slow SUCCEEDED

    python audit/evaluations/probe_parallel_leaf_not_terminal.py
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

from stabilize import StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.tasks.interface import RetryableTask, Task  # noqa: E402

STARTED: dict[str, float] = {}


class Fast(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class Slow(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        first = STARTED.setdefault(stage.id, time.monotonic())
        if time.monotonic() - first < 3.0:
            return TaskResult.running()
        return TaskResult.success()


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, retries in (("A budget 0.6 s", 3), ("B CONTROL budget 20 s", 100)):
            STARTED.clear()
            os.environ["STABILIZE_MAX_STAGE_WAIT_RETRIES"] = str(retries)
            os.environ["STABILIZE_HANDLER_RETRY_DELAY_S"] = "0.2"
            registry = TaskRegistry()
            registry.register("fast", Fast)
            registry.register("slow", Slow)
            with factory() as (store, queue):
                proc = processor(
                    store,
                    queue,
                    registry,
                    max_stage_wait_retries=retries,
                    handler_retry_delay_seconds=0.2,
                    task_backoff_min_delay_ms=100,
                    task_backoff_max_delay_ms=100,
                )
                wid = start(store, queue, workflow([stage("fast", ["fast"]), stage("slow", ["slow"])]))
                wf = drive(proc, store, wid, timeout=20, idle=2.0)["workflow"]
            slow = next(s for s in wf.stages if s.ref_id == "slow")
            ok = wf.status.name == "SUCCEEDED" and slow.status.name == "SUCCEEDED"
            print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {snapshot(wf)} (want SUCCEEDED, slow SUCCEEDED)")
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — a running branch is not failed by the completion wait budget")
    return 0


if __name__ == "__main__":
    sys.exit(main())
