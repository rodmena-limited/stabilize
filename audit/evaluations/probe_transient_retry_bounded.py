"""A task that keeps raising TransientError must stop after max_attempts (#62).

0.31.0 compared message.attempts against max_attempts, but every queue
delivery reset message.attempts to the row's delivery count (1 for a freshly
pushed retry message), so the counter never advanced and the task retried
forever while its workflow stayed RUNNING.

  A  a task raising TransientError on every call reaches TERMINAL, having
     executed exactly max_attempts (10) times, on SQLite and PostgreSQL
  B  CONTROL: a task that fails transiently twice and then succeeds reaches
     SUCCEEDED after 3 executions -- the bound must not cut retries short
  C  CONTROL: a PermanentError is TERMINAL after 1 execution

    python audit/evaluations/probe_transient_retry_bounded.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.errors import PermanentError, TransientError  # noqa: E402
from stabilize.resilience.config import HandlerConfig  # noqa: E402

CALLS: dict[str, int] = {}
EXPECTED_MAX = 10
BUDGET_SECONDS = 60.0


class AlwaysTransient(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        CALLS[stage.execution.id] = CALLS.get(stage.execution.id, 0) + 1
        raise TransientError("upstream unavailable")


class TransientTwice(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        n = CALLS[stage.execution.id] = CALLS.get(stage.execution.id, 0) + 1
        if n <= 2:
            raise TransientError("blip")
        return TaskResult.success()


class Permanent(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        CALLS[stage.execution.id] = CALLS.get(stage.execution.id, 0) + 1
        raise PermanentError("bad input")


def _run(store, queue, task_name: str) -> tuple[WorkflowStatus, int, float]:
    registry = TaskRegistry()
    registry.register("always_transient", AlwaysTransient)
    registry.register("transient_twice", TransientTwice)
    registry.register("permanent", Permanent)
    cfg = HandlerConfig(task_backoff_min_delay_ms=10, task_backoff_max_delay_ms=20)
    processor = QueueProcessor(queue, store=store, task_registry=registry, handler_config=cfg)
    wf = Workflow.create(
        application="probe",
        name="retry-bound",
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
    start = time.monotonic()
    while time.monotonic() - start < BUDGET_SECONDS:
        processor.process_all(timeout=2.0)
        status = store.retrieve(wf.id).status
        if status.is_complete:
            break
    return store.retrieve(wf.id).status, CALLS.get(wf.id, 0), time.monotonic() - start


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for task_name, want_status, want_calls in (
            ("always_transient", WorkflowStatus.TERMINAL, EXPECTED_MAX),
            ("transient_twice", WorkflowStatus.SUCCEEDED, 3),
            ("permanent", WorkflowStatus.TERMINAL, 1),
        ):
            with factory() as (store, queue):
                status, calls, took = _run(store, queue, task_name)
            ok = status == want_status and calls == want_calls
            mark = "PASS" if ok else "FAIL"
            print(
                f"[{mark}] {name} {task_name}: status={status.name} executions={calls} "
                f"(want {want_status.name}/{want_calls}) in {took:.1f}s"
            )
            if not ok:
                failures.append(f"{name}/{task_name}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — transient retries are bounded by max_attempts and still allowed below it")
    return 0


if __name__ == "__main__":
    sys.exit(main())
