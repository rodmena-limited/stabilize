"""A task's own exception is reported with the error code of that exception.

When a task raises, the engine records task_exception_details with an
error_code from classify_error. Task execution runs inside a bulkman bulkhead;
the error code must describe the task's exception, not the bulkhead that
carried it. The expected code is classify_error applied to the same exception
type raised outside the engine.

  A  Task raises ValueError: error_code == classify_error(ValueError()), not BULKHEAD_FULL
  B  RetryableTask raises KeyError: error_code == classify_error(KeyError())
  C  CONTROL: the recorded error text contains the exception's message

    python audit/evaluations/probe_raw_exception_error_code.py
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import drive, processor, stage, start, workflow  # noqa: E402

from stabilize import StageExecution, TaskRegistry, TaskResult  # noqa: E402
from stabilize.error_codes import classify_error  # noqa: E402
from stabilize.tasks.interface import RetryableTask, Task  # noqa: E402


class RaisesValue(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        raise ValueError("bad input 4711")


class RaisesKey(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=50)

    def execute(self, stage: StageExecution) -> TaskResult:
        raise KeyError("missing 4712")


def _details(wf: Any) -> dict[str, Any]:
    task = wf.stages[0].tasks[0]
    return ((task.task_exception_details or {}).get("exception") or {}).get("details") or {}


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, key, exc, needle in (
            ("A Task ValueError", "value", ValueError("x"), "4711"),
            ("B RetryableTask KeyError", "key", KeyError("x"), "4712"),
        ):
            registry = TaskRegistry()
            registry.register("value", RaisesValue)
            registry.register("key", RaisesKey)
            with factory() as (store, queue):
                proc = processor(store, queue, registry)
                wid = start(store, queue, workflow([stage("s", [key])]))
                wf = drive(proc, store, wid, timeout=10)["workflow"]
            details = _details(wf)
            want = classify_error(exc).value
            got = details.get("error_code")
            ok = got == want
            print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: error_code={got} (want {want}); workflow={wf.status.name}")
            if not ok:
                failures.append(f"{name}/{label}")
            text_ok = needle in str(details.get("error", ""))
            print(f"[{'PASS' if text_ok else 'FAIL'}] {name} C CONTROL {label} error text recorded: {str(details.get('error'))[:80]!r}")
            if not text_ok:
                failures.append(f"{name}/C {label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — task exceptions carry their own error code")
    return 0


if __name__ == "__main__":
    sys.exit(main())
