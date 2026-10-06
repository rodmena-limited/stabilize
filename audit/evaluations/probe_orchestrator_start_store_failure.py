"""Orchestrator.start must not push StartWorkflow for a workflow it failed to store (#65).

0.31.0 wrapped store.store(execution) in `except Exception: pass` to tolerate a
caller that had already stored the workflow, then pushed StartWorkflow
regardless. A real store failure therefore produced a queued StartWorkflow for
a workflow that does not exist, and start() returned as if it had worked.

  A  store() raising for a NEW workflow -> start() raises, nothing is queued
  B  CONTROL: a workflow the caller already stored -> start() succeeds, it runs
  C  CONTROL: a new workflow with a healthy store -> start() succeeds, it runs

    python audit/evaluations/probe_orchestrator_start_store_failure.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStatus,
)
from stabilize.persistence.store import WorkflowNotFoundError  # noqa: E402
from stabilize.tasks.shell import ShellTask  # noqa: E402


def _wf() -> Workflow:
    return Workflow.create(
        application="probe",
        name="start",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                context={"command": "true"},
                tasks=[TaskExecution.create("t", "shell", stage_start=True, stage_end=True)],
            )
        ],
    )


def _runs(store, queue, wf: Workflow) -> WorkflowStatus:
    registry = TaskRegistry()
    registry.register("shell", ShellTask)
    QueueProcessor(queue, store=store, task_registry=registry).process_all(timeout=10.0)
    return store.retrieve(wf.id).status


def main() -> int:
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        with factory() as (store, queue):
            wf = _wf()
            original = store.store

            def broken(execution, _original=original):
                raise RuntimeError("could not write: disk full")

            store.store = broken
            raised = None
            try:
                Orchestrator(queue, store=store).start(wf)
            except Exception as e:
                raised = e
            store.store = original
            queued = queue.size()
            try:
                store.retrieve(wf.id)
                exists = True
            except WorkflowNotFoundError:
                exists = False
            ok = raised is not None and queued == 0 and not exists
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} A store failure: raised={type(raised).__name__ if raised else None} "
                f"queued={queued} workflow_exists={exists}"
            )
            if not ok:
                failures.append(f"{name}/A")
            queue.clear()

            pre = _wf()
            store.store(pre)
            Orchestrator(queue, store=store).start(pre)
            status = _runs(store, queue, pre)
            ok = status == WorkflowStatus.SUCCEEDED
            print(f"[{'PASS' if ok else 'FAIL'}] {name} B pre-stored workflow: {status.name}")
            if not ok:
                failures.append(f"{name}/B")

            new = _wf()
            Orchestrator(queue, store=store).start(new)
            status = _runs(store, queue, new)
            ok = status == WorkflowStatus.SUCCEEDED
            print(f"[{'PASS' if ok else 'FAIL'}] {name} C new workflow: {status.name}")
            if not ok:
                failures.append(f"{name}/C")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — start() refuses to queue a workflow it could not store")
    return 0


if __name__ == "__main__":
    sys.exit(main())
