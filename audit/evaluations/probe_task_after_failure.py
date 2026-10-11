"""A task that ends the stage in failure stops the stage: the next task does not start.

TaskResult.terminal() documents "The stage and pipeline will fail". A stage runs
its tasks in sequence, so a task after a failed one is work the caller expects
not to happen: a deploy after a failed precondition, a charge after a failed
validation. Each case runs one stage [first, side_effect] and counts how many
times side_effect executed.

  A  first returns TaskResult.terminal(): side_effect executions 0
  B  first raises a non-transient exception: side_effect executions 0
  C  first returns TaskResult.stopped(): side_effect executions 0
  D  first returns TaskResult.canceled(): side_effect executions 0
  E  continuePipelineOnFailure=True and first returns terminal (converted to
     FAILED_CONTINUE): side_effect executions 0, the stage ends FAILED_CONTINUE
  F  CONTROL: first succeeds: side_effect executions 1, workflow SUCCEEDED

    python audit/evaluations/probe_task_after_failure.py
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

COUNT = {"side": 0}


class SideEffect(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        COUNT["side"] += 1
        return TaskResult.success()


class Terminal(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("precondition failed")


class Raises(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        raise ValueError("precondition failed")


class Stopped(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.stopped()


class Canceled(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.canceled()


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


CASES = [
    ("A terminal", "terminal", {}, 0, None),
    ("B raises", "raises", {}, 0, None),
    ("C stopped", "stopped", {}, 0, None),
    ("D canceled", "canceled", {}, 0, None),
    ("E continuePipelineOnFailure terminal", "terminal", {"continuePipelineOnFailure": True}, 0, WorkflowStatus.FAILED_CONTINUE),
    ("F CONTROL success", "ok", {}, 1, WorkflowStatus.SUCCEEDED),
]


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, first, context, want_side, want_stage in CASES:
            COUNT["side"] = 0
            registry = TaskRegistry()
            for key, cls in (("terminal", Terminal), ("raises", Raises), ("stopped", Stopped), ("canceled", Canceled), ("ok", Ok), ("side", SideEffect)):
                registry.register(key, cls)
            with factory() as (store, queue):
                proc = processor(store, queue, registry)
                wid = start(store, queue, workflow([stage("s", [first, "side"], context)]))
                out = drive(proc, store, wid)
                wf = out["workflow"]
            stage_status = wf.stages[0].status
            ok = COUNT["side"] == want_side and (want_stage is None or stage_status == want_stage)
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} {label}: side_effect executions={COUNT['side']} (want {want_side}); "
                f"{snapshot(wf)}"
            )
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — no task starts after a task that failed its stage")
    return 0


if __name__ == "__main__":
    sys.exit(main())
