"""SkippableTask.is_enabled is evaluated once per task, whatever name it is registered under.

StartTask asks is_enabled() before the task first runs. SkippableTask.execute
asks it again on every execution, so a polling task is re-evaluated on each poll
and a predicate that changes while the task runs (a flag service, a clock)
skips a task that has already started its work. StartTask also looks the task
up with registry.get(implementing_class) while RunTask uses get_by_class, so a
task whose implementing_class is a dotted path is never asked at StartTask.

  A  enabled task polling 3 times: is_enabled called 1
  B  disabled task with implementing_class "probe.pkg.Gate" (registered "Gate"):
     task SKIPPED, do_execute 0, is_enabled called 1, task never RUNNING
  C  CONTROL disabled task registered under its implementing_class: SKIPPED,
     do_execute 0

    python audit/evaluations/probe_skip_decided_once.py
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

CALLS = {"enabled": 0, "execute": 0, "running_seen": 0}


class Gate(SkippableTask, RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=50)

    def is_enabled(self, stage: StageExecution) -> bool:
        CALLS["enabled"] += 1
        return bool(stage.context.get("enabled", True))

    def do_execute(self, stage: StageExecution) -> TaskResult:
        CALLS["execute"] += 1
        CALLS["running_seen"] += 1
        polls = int(stage.context.get("polls", 0)) + 1
        if polls < 3:
            return TaskResult.running(context={"polls": polls})
        return TaskResult.success()


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, impl, enabled in (
            ("A enabled polling task", "gate", True),
            ("B disabled dotted implementing_class", "probe.pkg.Gate", False),
            ("C CONTROL disabled exact name", "gate", False),
        ):
            CALLS.update(enabled=0, execute=0, running_seen=0)
            registry = TaskRegistry()
            registry.register("gate", Gate)
            registry.register("Gate", Gate)
            with factory() as (store, queue):
                proc = processor(store, queue, registry, task_backoff_min_delay_ms=50, task_backoff_max_delay_ms=50)
                wid = start(store, queue, workflow([stage("s", [impl], {"enabled": enabled})]))
                wf = drive(proc, store, wid, timeout=10)["workflow"]
            task = wf.stages[0].tasks[0]
            if label.startswith("A"):
                ok = wf.status.name == "SUCCEEDED" and CALLS["enabled"] == 1 and CALLS["execute"] == 3
                want = "SUCCEEDED, is_enabled 1, do_execute 3"
            elif label.startswith("B"):
                ok = task.status.name == "SKIPPED" and CALLS["execute"] == 0 and CALLS["enabled"] == 1 and task.start_time is None
                want = "SKIPPED, do_execute 0, is_enabled 1, never started"
            else:
                ok = task.status.name == "SKIPPED" and CALLS["execute"] == 0
                want = "SKIPPED, do_execute 0"
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} {label}: {snapshot(wf)} is_enabled={CALLS['enabled']} "
                f"do_execute={CALLS['execute']} start_time={task.start_time} (want {want})"
            )
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — the skip decision is taken once")
    return 0


if __name__ == "__main__":
    sys.exit(main())
