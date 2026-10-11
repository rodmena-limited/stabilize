"""A task skipped at StartTask (SkippableTask.is_enabled False) lets its stage complete.

StartTask marks a disabled SkippableTask SKIPPED and pushes CompleteTask(SKIPPED).
That CompleteTask must start the next task or complete the stage. A recovery
sweep must also not leave a stage in that state forever.

  A  stage [on, off]: workflow SUCCEEDED
  B  stage [off, on]: workflow SUCCEEDED, "on" executed once
  C  stage [on, off] after two recovery sweeps: workflow SUCCEEDED
  D  CONTROL stage [on, offrun] where offrun returns TaskResult.skipped(): SUCCEEDED

    python audit/evaluations/probe_start_task_skip.py
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
from stabilize.tasks.interface import SkippableTask  # noqa: E402

COUNT: dict[str, int] = {}


class Off(SkippableTask):
    def is_enabled(self, stage: StageExecution) -> bool:
        return False

    def do_execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class OffAtRun(SkippableTask):
    def do_execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.skipped()


class On(SkippableTask):
    def do_execute(self, stage: StageExecution) -> TaskResult:
        COUNT[stage.execution.id] = COUNT.get(stage.execution.id, 0) + 1
        return TaskResult.success()


CASES = [
    ("A [on, off]", ["on", "off"], 0, 1),
    ("B [off, on]", ["off", "on"], 0, 1),
    ("C [on, off] after two recovery sweeps", ["on", "off"], 2, 1),
    ("D CONTROL [on, offrun]", ["on", "offrun"], 0, 1),
]


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, names, sweeps, want_on in CASES:
            registry = TaskRegistry()
            registry.register("off", Off)
            registry.register("offrun", OffAtRun)
            registry.register("on", On)
            with factory() as (store, queue):
                proc = processor(store, queue, registry)
                proc.config.recovery_application = "probe"
                wid = start(store, queue, workflow([stage("s", names)]))
                wf = drive(proc, store, wid, timeout=5, idle=0.5)["workflow"]
                for _ in range(sweeps):
                    proc.run_recovery()
                    wf = drive(proc, store, wid, timeout=5, idle=0.5)["workflow"]
            on = COUNT.get(wid, 0)
            ok = wf.status.name == "SUCCEEDED" and on == want_on
            print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {snapshot(wf)} on x{on} (want SUCCEEDED, on x{want_on})")
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — a StartTask skip completes its stage")
    return 0


if __name__ == "__main__":
    sys.exit(main())
