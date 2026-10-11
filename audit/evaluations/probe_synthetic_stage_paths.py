"""Synthetic before-stages gate their parent, propagate failure, and are not failed by a wait budget.

A parent stage with STAGE_BEFORE synthetic stages runs its own tasks only after
every before-stage completes.

  A  N2: a recovery sweep while the before-stage is still polling does not
     start the parent's task early: parent task starts after before-stage end
  B  N8: a before-stage ending FAILED_CONTINUE (continuePipelineOnFailure)
     lets the workflow reach a terminal status within 10 s without recovery
  C  B86-2: two parallel before-stages, one polling 3 s, with
     STABILIZE_MAX_STAGE_WAIT_RETRIES=3 and STABILIZE_HANDLER_RETRY_DELAY_S=0.2:
     workflow SUCCEEDED, slow before-stage SUCCEEDED
  D  CONTROL one fast before-stage: SUCCEEDED, before-stage ended before the parent task started

    python audit/evaluations/probe_synthetic_stage_paths.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402
from _engine import drive, processor, snapshot, start  # noqa: E402

from stabilize import StageExecution, TaskExecution, TaskRegistry, TaskResult, Workflow  # noqa: E402
from stabilize.models.stage import SyntheticStageOwner  # noqa: E402
from stabilize.resilience.config import reset_handler_config  # noqa: E402
from stabilize.tasks.interface import RetryableTask, Task  # noqa: E402

TIMES: dict[str, list[float]] = {}


def _mark(key: str) -> None:
    TIMES.setdefault(key, []).append(time.monotonic())


class Main(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        _mark("main")
        return TaskResult.success()


class Fast(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        _mark(f"end:{stage.ref_id}")
        return TaskResult.success()


class Slow(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        first = float(stage.context.get("first", 0) or 0) or time.time()
        if time.time() - first < float(stage.context.get("hold", 1.5)):
            return TaskResult.running(context={"first": first})
        _mark(f"end:{stage.ref_id}")
        return TaskResult.success()


class Fails(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("before-stage failed")


def _task(impl: str) -> TaskExecution:
    return TaskExecution.create(impl, impl, stage_start=True, stage_end=True)


def _wf(befores: list[tuple[str, str, dict[str, Any], set[str]]]) -> Workflow:
    parent = StageExecution(ref_id="parent", type="probe", name="parent", tasks=[_task("main")])
    stages = [parent]
    for ref, impl, ctx, needs in befores:
        b = StageExecution(
            ref_id=ref,
            type="probe",
            name=ref,
            context=dict(ctx),
            requisite_stage_ref_ids=set(needs),
            synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
            tasks=[_task(impl)],
        )
        stages.append(b)
    wf = Workflow.create(application="probe", name="synthetic", stages=stages)
    for s in wf.stages[1:]:
        s.parent_stage_id = parent.id
    return wf


def _registry() -> TaskRegistry:
    r = TaskRegistry()
    r.register("main", Main)
    r.register("fast", Fast)
    r.register("slow", Slow)
    r.register("fails", Fails)
    return r


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []

    def report(ok: bool, label: str, detail: str) -> None:
        print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")
        if not ok:
            failures.append(label)

    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue

        TIMES.clear()
        with factory() as (store, queue):
            proc = processor(store, queue, _registry(), task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100)
            proc.config.recovery_application = "probe"
            wid = start(store, queue, _wf([("before", "slow", {"hold": 1.5}, set())]))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                wf = store.retrieve(wid)
                b = next(s for s in wf.stages if s.ref_id == "before")
                if b.status.name == "RUNNING" and b.tasks[0].status.name == "RUNNING":
                    break
                try:
                    if not proc.process_one():
                        time.sleep(0.02)
                except Exception:
                    pass
            proc.run_recovery()
            wf = drive(proc, store, wid, timeout=10)["workflow"]
        main_t = TIMES.get("main", [None])[0]
        end_t = TIMES.get("end:before", [None])[0]
        ok = main_t is not None and end_t is not None and main_t >= end_t and len(TIMES.get("main", [])) == 1
        report(ok, f"{name} A recovery during before-stage", f"{snapshot(wf)} main ran {len(TIMES.get('main', []))}x, after before-stage end={ok}")

        TIMES.clear()
        with factory() as (store, queue):
            proc = processor(store, queue, _registry())
            wid = start(store, queue, _wf([("before", "fails", {"continuePipelineOnFailure": True}, set())]))
            wf = drive(proc, store, wid, timeout=10, idle=2.0)["workflow"]
        report(wf.status.is_complete, f"{name} B FAILED_CONTINUE before-stage", f"{snapshot(wf)} (want a terminal workflow status)")

        TIMES.clear()
        os.environ["STABILIZE_MAX_STAGE_WAIT_RETRIES"] = "3"
        os.environ["STABILIZE_HANDLER_RETRY_DELAY_S"] = "0.2"
        reset_handler_config()
        try:
            with factory() as (store, queue):
                proc = processor(store, queue, _registry(), task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100)
                wid = start(store, queue, _wf([("b1", "fast", {}, set()), ("b2", "slow", {"hold": 3.0}, set())]))
                wf = drive(proc, store, wid, timeout=20, idle=2.0)["workflow"]
        finally:
            os.environ.pop("STABILIZE_MAX_STAGE_WAIT_RETRIES", None)
            os.environ.pop("STABILIZE_HANDLER_RETRY_DELAY_S", None)
            reset_handler_config()
        b2 = next(s for s in wf.stages if s.ref_id == "b2")
        report(wf.status.name == "SUCCEEDED" and b2.status.name == "SUCCEEDED", f"{name} C parallel before-stages, slow sibling", f"{snapshot(wf)} (want SUCCEEDED, b2 SUCCEEDED)")

        TIMES.clear()
        with factory() as (store, queue):
            proc = processor(store, queue, _registry())
            wid = start(store, queue, _wf([("before", "fast", {}, set())]))
            wf = drive(proc, store, wid, timeout=10)["workflow"]
        main_t = TIMES.get("main", [None])[0]
        end_t = TIMES.get("end:before", [None])[0]
        ok = wf.status.name == "SUCCEEDED" and main_t is not None and end_t is not None and main_t >= end_t
        report(ok, f"{name} D CONTROL one fast before-stage", f"{snapshot(wf)}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — before-stages gate, fail and wait correctly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
