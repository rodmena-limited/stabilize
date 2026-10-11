"""on_cleanup runs once per stage completion even when the completing write conflicts.

CompleteStage retries its handler on ConcurrencyError (optimistic locking). Task
on_cleanup releases resources (a container, a lease); calling it once per retry
releases or notifies more than once. One version conflict is injected on the
first store_stage of the stage in its SUCCEEDED state.

  A  one injected ConcurrencyError at stage completion: on_cleanup called 1, workflow SUCCEEDED
  B  CONTROL no conflict: on_cleanup called 1, workflow SUCCEEDED

    python audit/evaluations/probe_cleanup_once_on_conflict.py
"""

from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import drive, processor, snapshot, stage, start, workflow  # noqa: E402

from stabilize import StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.errors.transient import ConcurrencyError  # noqa: E402
from stabilize.models.status import WorkflowStatus  # noqa: E402
from stabilize.tasks.interface import Task  # noqa: E402

CALLS = {"cleanup": 0, "armed": False, "fired": 0}


class Work(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()

    def on_cleanup(self, stage: StageExecution) -> None:
        CALLS["cleanup"] += 1


class _Txn:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def store_stage(self, stage: Any, *args: Any, **kwargs: Any) -> Any:
        if CALLS["armed"] and stage.status == WorkflowStatus.SUCCEEDED and stage.parent_stage_id is None:
            CALLS["armed"] = False
            CALLS["fired"] += 1
            raise ConcurrencyError("version changed")
        return self._inner.store_stage(stage, *args, **kwargs)

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)


def _inject(store: Any) -> None:
    original = store.transaction

    @contextmanager
    def transaction(queue: Any = None) -> Any:
        with original(queue) as txn:
            yield _Txn(txn)

    store.transaction = transaction


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, inject in (("A one conflict at completion", True), ("B CONTROL no conflict", False)):
            CALLS.update(cleanup=0, armed=inject, fired=0)
            registry = TaskRegistry()
            registry.register("work", Work)
            with factory() as (store, queue):
                proc = processor(store, queue, registry)
                _inject(store)
                wid = start(store, queue, workflow([stage("s", ["work"])]))
                wf = drive(proc, store, wid, timeout=10)["workflow"]
            ok = CALLS["cleanup"] == 1 and wf.status.name == "SUCCEEDED" and CALLS["fired"] == int(inject)
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} {label}: on_cleanup={CALLS['cleanup']} conflicts={CALLS['fired']} "
                f"{snapshot(wf)} (want 1, SUCCEEDED)"
            )
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — on_cleanup runs once across a version conflict")
    return 0


if __name__ == "__main__":
    sys.exit(main())
