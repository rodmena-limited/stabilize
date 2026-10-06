"""Recovery examines every pending workflow in its window, not only the newest page (#64).

0.31.0 built WorkflowCriteria(page_size=batch_size=100) and both backends
applied ORDER BY start_time DESC NULLS LAST LIMIT 100 with no pagination, so
recovery looked at the newest 100 pending workflows and never at the rest.

Setup: 150 workflows stored and never started (a crash between store() and
the StartWorkflow push), plus one RUNNING workflow whose start_time is 48 h
old, outside the default 24 h window.

  A  all 150 stored-not-started workflows are recovered (StartWorkflow pushed)
  B  the application-filtered path recovers all 150 too
  C  CONTROL: the 48 h old workflow is NOT examined -- the window still applies
  D  CONTROL: recovery run twice pushes nothing new the second time... is NOT
     asserted: NOT_STARTED recovery is not deduplicated against the queue, and
     this probe does not claim it is.

    python audit/evaluations/probe_recovery_examines_all.py
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402

from stabilize import StageExecution, TaskExecution, Workflow, WorkflowStatus  # noqa: E402
from stabilize.recovery import WorkflowRecovery  # noqa: E402

PENDING = 150


def _wf(app: str) -> Workflow:
    return Workflow.create(
        application=app,
        name="recovery-probe",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("t", "shell", stage_start=True, stage_end=True)],
            )
        ],
    )


def _check(store, queue, application: str | None) -> tuple[bool, str]:
    app = f"probe-rec-{int(time.time() * 1000)}"
    pending_ids = set()
    for _ in range(PENDING):
        wf = _wf(app)
        store.store(wf)
        pending_ids.add(wf.id)
    old = _wf(app)
    old.status = WorkflowStatus.RUNNING
    old.start_time = int(time.time() * 1000) - 48 * 3600 * 1000
    old.stages[0].status = WorkflowStatus.RUNNING
    store.store(old)

    results = WorkflowRecovery(store, queue).recover_pending_workflows(
        application=app if application else None
    )
    recovered = {r.workflow_id for r in results if r.status == "recovered"}
    examined = {r.workflow_id for r in results}
    missing = pending_ids - recovered
    ok = not missing and old.id not in examined
    return ok, (
        f"recovered {len(pending_ids & recovered)}/{PENDING} stored-not-started, "
        f"48h-old workflow examined={old.id in examined}"
    )


def main() -> int:
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, application in (("all-applications", None), ("application-filter", "x")):
            with factory() as (store, queue):
                ok, detail = _check(store, queue, application)
            print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {detail}")
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — recovery reaches every pending workflow in its window and none outside it")
    return 0


if __name__ == "__main__":
    sys.exit(main())
