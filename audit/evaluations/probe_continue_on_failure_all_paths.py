"""continuePipelineOnFailure applies to every way a task can fail.

A stage with context continuePipelineOnFailure=True ends FAILED_CONTINUE when
its task fails, and its downstream stage still runs. That must hold whether the
task returned TaskResult.terminal(), timed out, or could not be resolved.

Stages: "s" (continuePipelineOnFailure) -> "after".

  A  s's RetryableTask exceeds its total timeout: s FAILED_CONTINUE, after SUCCEEDED
  B  s's implementing_class is not registered: s FAILED_CONTINUE, after SUCCEEDED
  C  CONTROL s's task returns TaskResult.terminal(): s FAILED_CONTINUE, after SUCCEEDED

    python audit/evaluations/probe_continue_on_failure_all_paths.py
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import drive, processor, snapshot, stage, start, workflow  # noqa: E402

from stabilize import StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.tasks.interface import RetryableTask, Task  # noqa: E402


class Slow(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(milliseconds=300)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=50)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()


class Terminal(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("failed")


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
        for label, impl in (
            ("A total timeout", "slow"),
            ("B task not registered", "not_registered"),
            ("C CONTROL terminal result", "terminal"),
        ):
            registry = TaskRegistry()
            registry.register("slow", Slow)
            registry.register("terminal", Terminal)
            registry.register("ok", Ok)
            with factory() as (store, queue):
                proc = processor(store, queue, registry, task_backoff_min_delay_ms=50, task_backoff_max_delay_ms=50)
                wf = workflow(
                    [
                        stage("s", [impl], {"continuePipelineOnFailure": True}),
                        stage("after", ["ok"], needs={"s"}),
                    ]
                )
                wid = start(store, queue, wf)
                wf = drive(proc, store, wid, timeout=10, idle=1.5)["workflow"]
            s = next(x for x in wf.stages if x.ref_id == "s")
            after = next(x for x in wf.stages if x.ref_id == "after")
            ok = s.status.name == "FAILED_CONTINUE" and after.status.name == "SUCCEEDED"
            print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {snapshot(wf)} (want s FAILED_CONTINUE, after SUCCEEDED)")
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — continuePipelineOnFailure holds on every failure path")
    return 0


if __name__ == "__main__":
    sys.exit(main())
