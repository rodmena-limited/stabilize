"""A REDIRECT result without a target stage does not leave its stage running forever.

TaskResult(status=REDIRECT) without target_stage_ref_id is not a jump: there is
nowhere to go. It must end the stage (as an error) rather than leave the stage
RUNNING with no message queued for it.

  A  last task returns REDIRECT without a target: the workflow reaches a terminal status within 10 s
  B  CONTROL last task returns success: SUCCEEDED

    python audit/evaluations/probe_redirect_without_target.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import drive, processor, snapshot, stage, start, workflow  # noqa: E402

from stabilize import StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.models.status import WorkflowStatus  # noqa: E402
from stabilize.tasks.interface import Task  # noqa: E402


class Redirect(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult(status=WorkflowStatus.REDIRECT)


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, impl in (("A REDIRECT without target", "redirect"), ("B CONTROL success", "ok")):
            registry = TaskRegistry()
            registry.register("redirect", Redirect)
            registry.register("ok", Ok)
            with factory() as (store, queue):
                proc = processor(store, queue, registry)
                wid = start(store, queue, workflow([stage("s", ["ok", impl])]))
                wf = drive(proc, store, wid, timeout=10, idle=1.5)["workflow"]
            ok = wf.status.is_complete and (impl == "redirect" or wf.status.name == "SUCCEEDED")
            print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {snapshot(wf)} (want a terminal workflow status)")
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — REDIRECT without a target ends its stage")
    return 0


if __name__ == "__main__":
    sys.exit(main())
