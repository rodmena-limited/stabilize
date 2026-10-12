"""A user hook that raises ends its task once; it is neither repeated nor left stranded.

Task hooks run inside the engine's handlers: RetryableTask.on_timeout when the
task's total timeout passes, SkippableTask.is_enabled at StartTask. If a hook
raises, the task must still reach a terminal status, and the hook must not be
called again on every redelivery of the message.

  A  on_timeout raises: workflow TERMINAL within 10 s, on_timeout called once
  B  is_enabled raises: workflow TERMINAL within 10 s, is_enabled called once
  C  CONTROL on_timeout returns None: workflow TERMINAL, on_timeout called once

    python audit/evaluations/probe_raising_task_hooks.py
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
from stabilize.tasks.interface import RetryableTask, SkippableTask  # noqa: E402

CALLS = {"hook": 0}


class _Polls(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(milliseconds=300)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=50)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()


class TimeoutRaises(_Polls):
    def on_timeout(self, stage: StageExecution) -> TaskResult | None:
        CALLS["hook"] += 1
        raise RuntimeError("cleanup service unreachable")


class TimeoutNone(_Polls):
    def on_timeout(self, stage: StageExecution) -> TaskResult | None:
        CALLS["hook"] += 1
        return None


class EnabledRaises(SkippableTask):
    def is_enabled(self, stage: StageExecution) -> bool:
        CALLS["hook"] += 1
        raise RuntimeError("feature flag service unreachable")

    def do_execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, key in (
            ("A on_timeout raises", "timeout_raises"),
            ("B is_enabled raises", "enabled_raises"),
            ("C CONTROL on_timeout returns None", "timeout_none"),
        ):
            CALLS["hook"] = 0
            registry = TaskRegistry()
            registry.register("timeout_raises", TimeoutRaises)
            registry.register("timeout_none", TimeoutNone)
            registry.register("enabled_raises", EnabledRaises)
            with factory() as (store, queue):
                proc = processor(store, queue, registry, task_backoff_min_delay_ms=50, task_backoff_max_delay_ms=50)
                wid = start(store, queue, workflow([stage("s", [key])]))
                wf = drive(proc, store, wid, timeout=10, idle=1.5)["workflow"]
            ok = wf.status.name == "TERMINAL" and CALLS["hook"] == 1
            print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {snapshot(wf)} hook calls={CALLS['hook']} (want TERMINAL, 1)")
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — raising hooks end their task once")
    return 0


if __name__ == "__main__":
    sys.exit(main())
