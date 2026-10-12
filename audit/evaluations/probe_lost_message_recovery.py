"""After any single lifecycle message is lost, one recovery sweep finishes the workflow without re-running work.

A message can leave the queue without being handled: it exhausts its attempts
and is dead-lettered, or the dead-letter queue is purged. Recovery exists for
that case. For each message type of a one-stage, one-task workflow, the first
message of that type is consumed without being handled, one recovery sweep
runs, and the processor drains.

  <Type>  workflow SUCCEEDED and the task executed exactly once
          (StartWorkflow, StartStage, StartTask, RunTask, CompleteTask,
           CompleteStage, CompleteWorkflow)
  CONTROL nothing lost: SUCCEEDED, task executed once

    python audit/evaluations/probe_lost_message_recovery.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import processor, snapshot, stage, start, workflow  # noqa: E402

from stabilize import StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.queue.messages import get_message_type_name  # noqa: E402
from stabilize.tasks.interface import Task  # noqa: E402

COUNT = {"exec": 0}
TYPES = ["StartWorkflow", "StartStage", "StartTask", "RunTask", "CompleteTask", "CompleteStage", "CompleteWorkflow"]


class Work(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        COUNT["exec"] += 1
        return TaskResult.success(outputs={"done": True})


def _drain(proc: Any, store: Any, wid: str, drop: str | None, seconds: float) -> int:
    dropped = 0
    deadline = time.monotonic() + seconds
    idle_since = time.monotonic()
    while time.monotonic() < deadline:
        msg = proc.queue.poll_one()
        if msg is None:
            if store.retrieve(wid).status.is_complete or time.monotonic() - idle_since > 1.0:
                break
            time.sleep(0.02)
            continue
        idle_since = time.monotonic()
        if drop is not None and dropped == 0 and get_message_type_name(msg) == drop:
            proc.queue.ack(msg)
            dropped += 1
            continue
        try:
            proc._handle_message(msg)
            proc.queue.ack(msg)
        except Exception:
            proc.queue.reschedule(msg, timedelta(milliseconds=50))
    return dropped


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for drop in [None, *TYPES]:
            COUNT["exec"] = 0
            registry = TaskRegistry()
            registry.register("work", Work)
            with factory() as (store, queue):
                proc = processor(store, queue, registry)
                proc.config.recovery_application = "probe"
                wid = start(store, queue, workflow([stage("s", ["work"])]))
                dropped = _drain(proc, store, wid, drop, 10)
                proc.run_recovery()
                _drain(proc, store, wid, None, 10)
                wf = store.retrieve(wid)
            label = f"lost {drop}" if drop else "CONTROL nothing lost"
            ok = wf.status.name == "SUCCEEDED" and COUNT["exec"] == 1 and dropped == (1 if drop else 0)
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {snapshot(wf)} executions={COUNT['exec']} "
                f"dropped={dropped} (want SUCCEEDED, executions 1)"
            )
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — every single lost message is recovered without re-running the task")
    return 0


if __name__ == "__main__":
    sys.exit(main())
