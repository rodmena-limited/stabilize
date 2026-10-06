"""A store failure while saving a SUCCESSFUL task's result is not a task failure (#63).

0.31.0 saved the result inside the same try block that classifies task
exceptions, so a database error raised while committing a successful result
was handled as if the TASK had raised it: a non-transient error marked the
task TERMINAL after one successful execution, a transient one re-ran the task.

The fault is injected at the persistence boundary: the store's transaction()
raises once, on the first transaction opened after the task returned success.
Nothing inside the handler is patched.

  A  store unavailable (QueryCanceled / disk I/O)           -> SUCCEEDED, 1 execution
  B  transient store error (PoolTimeout / OperationalError) -> SUCCEEDED, 1 execution
  E  the store refuses the result itself (DataError family) -> TERMINAL with the reason,
     1 execution: a redelivery would be refused the same way, so holding it would leave
     the task RUNNING forever
  C  CONTROL: no injected fault                             -> SUCCEEDED, 1 execution
  D  CONTROL: the injection fired in A and B (counted), so the green is not a no-op

    python audit/evaluations/probe_result_persist_failure.py
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
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
from stabilize.resilience.config import HandlerConfig  # noqa: E402

STATE = {"armed": False, "fired": 0, "calls": 0, "error": None}


class Succeeds(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        STATE["calls"] += 1
        if STATE["error"] is not None:
            STATE["armed"] = True
        return TaskResult.success(outputs={"done": True})


def _inject(store) -> None:
    original = store.transaction

    @contextmanager
    def transaction(queue=None):
        if STATE["armed"]:
            STATE["armed"] = False
            STATE["fired"] += 1
            raise STATE["error"]
        with original(queue) as txn:
            yield txn

    store.transaction = transaction


def _errors(backend: str):
    if backend == "postgres":
        import psycopg.errors
        from psycopg_pool import PoolTimeout

        return [
            ("unavailable", psycopg.errors.QueryCanceled("canceling statement due to statement timeout")),
            ("transient", PoolTimeout("couldn't get a connection after 30.00 sec")),
            ("refused", psycopg.errors.UntranslatableCharacter("unsupported Unicode escape sequence")),
        ]
    return [
        ("unavailable", sqlite3.OperationalError("disk I/O error")),
        ("transient", sqlite3.OperationalError("unable to open database file")),
        ("refused", sqlite3.DataError("string or blob too big")),
    ]


def _run(store, queue) -> tuple[WorkflowStatus, int]:
    registry = TaskRegistry()
    registry.register("succeeds", Succeeds)
    cfg = HandlerConfig(task_backoff_min_delay_ms=10, task_backoff_max_delay_ms=20)
    processor = QueueProcessor(queue, store=store, task_registry=registry, handler_config=cfg)
    processor.config.retry_delay = processor.config.retry_delay.__class__(milliseconds=50)
    wf = Workflow.create(
        application="probe",
        name="persist-failure",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("t", "succeeds", stage_start=True, stage_end=True)],
            )
        ],
    )
    store.store(wf)
    Orchestrator(queue, store=store).start(wf)
    _inject(store)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            processor.process_all(timeout=1.0)
        except Exception:
            pass
        if store.retrieve(wf.id).status.is_complete:
            break
    return store.retrieve(wf.id).status, STATE["calls"]


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, err in [("control", None), *_errors(name)]:
            STATE.update(armed=False, fired=0, calls=0, error=err)
            with factory() as (store, queue):
                status, calls = _run(store, queue)
            fired_ok = (STATE["fired"] == 1) if err is not None else (STATE["fired"] == 0)
            want = WorkflowStatus.TERMINAL if label == "refused" else WorkflowStatus.SUCCEEDED
            ok = status == want and calls == 1 and fired_ok
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} {label}: status={status.name} "
                f"executions={calls} injected={STATE['fired']} "
                f"({type(err).__name__ if err else 'none'})"
            )
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — a failed save of a successful result neither fails nor re-runs the task")
    return 0


if __name__ == "__main__":
    sys.exit(main())
