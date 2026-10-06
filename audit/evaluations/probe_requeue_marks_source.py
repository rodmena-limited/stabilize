"""A re-queued RunTask does not fork a second execution chain when the processed mark fails.

When a task returns RUNNING (poll again) or raises TransientError (retry), the
handler commits the stage plus a NEW RunTask. 0.31.0 did not mark the SOURCE
message processed in that transaction, so if the processor's separate
processed-mark failed afterwards -- a database blip at that moment -- the
source was rescheduled, redelivered, executed the task again, and pushed a
second follow-up: two chains polling the same task until it finished.

Fault: the processor's post-handler store.mark_message_processed raises once,
for the RunTask that ran the task's first execution. The follow-up RunTask is
5 s away, so within the next 1.5 s a single chain executes the task exactly
once; a forked chain executes it twice and leaves two queued messages.

  A  RUNNING (5 s poll backoff): 1 execution in the window, 1 queued message
  B  TransientError (5 s retry backoff): 1 execution in the window, 1 queued message
  C  CONTROL: without the fault, the same counts
  D  the fault count is reported; once the source is marked inside the
     re-queue transaction, PostgreSQL skips the separate mark, so it need not
     fire -- that is the fix, not a vacuous pass. On 0.31.0 it fires.

    python audit/evaluations/probe_requeue_marks_source.py
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

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.errors import TransientError  # noqa: E402
from stabilize.resilience.config import HandlerConfig  # noqa: E402
from stabilize.tasks.interface import RetryableTask, Task  # noqa: E402

STATE: dict[str, Any] = {}


def _count_call() -> None:
    STATE["calls"] += 1
    if STATE["calls"] == 1:
        STATE["arm"] = STATE["inject"]


class Poller(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(seconds=5)

    def execute(self, stage: StageExecution) -> TaskResult:
        _count_call()
        return TaskResult.running()


class Flaky(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        _count_call()
        raise TransientError("blip")


def _inject(store: Any) -> None:
    original = store.mark_message_processed

    def mark_message_processed(message_id: str, handler_type: str | None = None, execution_id: str | None = None) -> None:
        if STATE["arm"] and handler_type == "RunTask":
            STATE["arm"] = False
            STATE["fired"] += 1
            raise ConnectionError("server closed the connection unexpectedly")
        original(message_id=message_id, handler_type=handler_type, execution_id=execution_id)

    store.mark_message_processed = mark_message_processed


def _run(store: Any, queue: Any, task_name: str) -> tuple[int, int]:
    registry = TaskRegistry()
    registry.register("poller", Poller)
    registry.register("flaky", Flaky)
    cfg = HandlerConfig(task_backoff_min_delay_ms=5000, task_backoff_max_delay_ms=5000)
    processor = QueueProcessor(queue, store=store, task_registry=registry, handler_config=cfg)
    processor.config.retry_delay = timedelta(milliseconds=50)
    wf = Workflow.create(
        application="probe",
        name="requeue",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("t", task_name, stage_start=True, stage_end=True)],
            )
        ],
    )
    store.store(wf)
    Orchestrator(queue, store=store).start(wf)
    _inject(store)
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        try:
            processor.process_one()
        except Exception:
            pass
        time.sleep(0.02)
    return STATE["calls"], queue.size()


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for task_name in ("poller", "flaky"):
            for inject in (False, True):
                STATE.update(calls=0, arm=False, fired=0, inject=inject)
                with factory() as (store, queue):
                    calls, queued = _run(store, queue, task_name)
                ok = calls == 1 and queued == 1 and STATE["fired"] <= int(inject)
                label = f"{task_name} {'mark-failure' if inject else 'control'}"
                print(
                    f"[{'PASS' if ok else 'FAIL'}] {name} {label}: executions={calls} queued={queued} "
                    f"(want 1/1) injected={STATE['fired']}"
                )
                if not ok:
                    failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — a failed processed-mark after a re-queue does not fork the task")
    return 0


if __name__ == "__main__":
    sys.exit(main())
