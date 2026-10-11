"""A task raising SystemExit is executed once and ends TERMINAL; the worker survives.

BaseException subclasses that are not Exception (SystemExit, KeyboardInterrupt,
GeneratorExit) pass through `except Exception` handlers. A task raising one
must still be recorded as a failure, once, not redelivered and re-executed
until its attempts run out.

  A  task raises SystemExit: executed 1 time, task TERMINAL, workflow complete,
     and the probe's own loop never sees the exception
  B  CONTROL task raises RuntimeError: executed 1 time, TERMINAL

    python audit/evaluations/probe_base_exception_task.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import processor, snapshot, stage, start, workflow  # noqa: E402

from stabilize import StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.tasks.interface import Task  # noqa: E402

COUNT = {"n": 0}


class Exits(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        COUNT["n"] += 1
        raise SystemExit(3)


class Raises(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        COUNT["n"] += 1
        raise RuntimeError("boom")


def main() -> int:
    import faulthandler

    faulthandler.dump_traceback_later(180, exit=True)
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, impl in (("A SystemExit", "exits"), ("B CONTROL RuntimeError", "raises")):
            COUNT["n"] = 0
            escaped = 0
            registry = TaskRegistry()
            registry.register("exits", Exits)
            registry.register("raises", Raises)
            with factory() as (store, queue):
                proc = processor(store, queue, registry)
                wid = start(store, queue, workflow([stage("s", [impl])]))
                deadline = time.monotonic() + 15
                idle = time.monotonic()
                while time.monotonic() < deadline:
                    try:
                        worked = proc.process_one()
                    except Exception:
                        worked = True
                    except BaseException:
                        escaped += 1
                        worked = True
                    if worked:
                        idle = time.monotonic()
                    elif store.retrieve(wid).status.is_complete or time.monotonic() - idle > 1.5:
                        break
                    else:
                        time.sleep(0.02)
                wf = store.retrieve(wid)
            task = wf.stages[0].tasks[0]
            ok = COUNT["n"] == 1 and task.status.name == "TERMINAL" and wf.status.is_complete and escaped == 0
            print(
                f"[{'PASS' if ok else 'FAIL'}] {name} {label}: executions={COUNT['n']} escaped={escaped} "
                f"{snapshot(wf)} (want 1, 0, TERMINAL)"
            )
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — BaseException from a task is a single recorded failure")
    return 0


if __name__ == "__main__":
    sys.exit(main())
