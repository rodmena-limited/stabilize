"""Work in flight under the previous release finishes under this one after an upgrade.

Two phases against one PostgreSQL database (STABILIZE_PROBE_DSN), each run by a
different interpreter:

  --phase old   (published previous release): migrate, start 6 workflows of
                3 shapes, and process until every polling task has polled
                twice; then exit with messages still queued
  --phase new   (this checkout): migrate (mg-up applies only newer
                migrations), process to completion and check

  A  every workflow started under the old release reaches SUCCEEDED
  B  each polling task resumes from its persisted poll count: total polls per
     task == 6 (at least 2 before), none restarted from zero
  C  CONTROL a workflow started under the new release in the same database SUCCEEDS

The task module is this file, so both interpreters register the same tasks.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)

CALLS: dict[str, int] = {}
STATE_FILE = Path(os.environ.get("UPGRADE_STATE_FILE", "/tmp/stabilize-upgrade-state.json"))


def _registry() -> Any:
    from stabilize import StageExecution, TaskRegistry, TaskResult
    from stabilize.tasks.interface import RetryableTask, Task

    class Poll(RetryableTask):
        def get_timeout(self) -> timedelta:
            return timedelta(minutes=10)

        def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
            return timedelta(milliseconds=200)

        def execute(self, stage: StageExecution) -> TaskResult:
            CALLS[stage.execution.id] = CALLS.get(stage.execution.id, 0) + 1
            polls = int(stage.context.get("polls", 0)) + 1
            if polls < 6:
                return TaskResult.running(context={"polls": polls})
            return TaskResult.success(outputs={"polls": polls})

    class Ok(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            return TaskResult.success(outputs={"ok": True})

    r = TaskRegistry()
    r.register("poll", Poll)
    r.register("ok", Ok)
    return r


def _workflows() -> list[Any]:
    from stabilize import StageExecution, TaskExecution, Workflow

    def st(ref: str, impls: list[str], needs: set[str] | None = None) -> StageExecution:
        n = len(impls)
        return StageExecution(
            ref_id=ref,
            type="upgrade",
            name=ref,
            requisite_stage_ref_ids=set(needs or set()),
            tasks=[TaskExecution.create(f"{i}-{x}", x, stage_start=i == 0, stage_end=i == n - 1) for i, x in enumerate(impls)],
        )

    shapes = [
        lambda: [st("a", ["poll"])],
        lambda: [st("a", ["ok"]), st("b", ["poll"], {"a"})],
        lambda: [st("a", ["poll"]), st("b", ["ok"]), st("c", ["ok"], {"a", "b"})],
    ]
    out = []
    for i in range(6):
        out.append(Workflow.create(application="upgrade", name=f"u{i}", stages=shapes[i % 3]()))
    return out


def _open(dsn: str) -> tuple[Any, Any, Any]:
    from stabilize import PostgresQueue, PostgresWorkflowStore, QueueProcessor

    store = PostgresWorkflowStore(dsn)
    queue = PostgresQueue(dsn, table_name="queue_messages")
    proc = QueueProcessor(queue, store=store, task_registry=_registry())
    proc.config.retry_delay = timedelta(milliseconds=50)
    proc.config.recovery_application = "upgrade"
    return store, queue, proc


def _polls(store: Any, wid: str) -> int:
    wf = store.retrieve(wid)
    return max(max(int(s.context.get("polls", 0) or 0), int(s.outputs.get("polls", 0) or 0)) for s in wf.stages)


def phase_old(dsn: str) -> int:
    from stabilize import Orchestrator
    from stabilize.cli.commands import mg_up

    mg_up(dsn)
    store, queue, proc = _open(dsn)
    queue.clear()
    ids = []
    for wf in _workflows():
        store.store(wf)
        Orchestrator(queue, store=store).start(wf)
        ids.append(wf.id)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not all(_polls(store, i) >= 2 for i in ids):
        try:
            if not proc.process_one():
                time.sleep(0.02)
        except Exception:
            pass
    before = {i: _polls(store, i) for i in ids}
    STATE_FILE.write_text(json.dumps({"ids": ids, "before": before}))
    print(f"[INFO] old phase: {len(ids)} workflows, polls before upgrade={sorted(before.values())}, queued={queue.size()}")
    return 0 if all(2 <= v < 6 for v in before.values()) else 1


def phase_new(dsn: str) -> int:
    import stabilize
    from stabilize import Orchestrator
    from stabilize.cli.commands import mg_up

    print(f"[INFO] new phase: stabilize {stabilize.__version__} from {stabilize.__file__}")
    mg_up(dsn)
    state = json.loads(STATE_FILE.read_text())
    store, queue, proc = _open(dsn)
    control = _workflows()[0]
    store.store(control)
    Orchestrator(queue, store=store).start(control)
    ids = state["ids"]
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and not all(store.retrieve(i).status.is_complete for i in [*ids, control.id]):
        try:
            if not proc.process_one():
                time.sleep(0.02)
        except Exception:
            pass
    failures = 0
    statuses = {i: store.retrieve(i).status.name for i in ids}
    ok_a = all(s == "SUCCEEDED" for s in statuses.values())
    print(f"[{'PASS' if ok_a else 'FAIL'}] A old-release workflows finish: {sorted(statuses.values())}")
    before = state["before"]
    resumed = {i: CALLS.get(i, 0) for i in ids}
    ok_b = all(resumed[i] == 6 - int(before[i]) for i in ids)
    detail = [f"{before[i]}+{resumed[i]}" for i in ids]
    print(f"[{'PASS' if ok_b else 'FAIL'}] B polls resume from persisted count (before+after, want sum 6): {detail}")
    c = store.retrieve(control.id).status.name
    ok_c = c == "SUCCEEDED"
    print(f"[{'PASS' if ok_c else 'FAIL'}] C CONTROL new-release workflow: {c}")
    failures = sum(not x for x in (ok_a, ok_b, ok_c))
    print("VERDICT: PASS — in-flight work survives the upgrade" if failures == 0 else "VERDICT: FAIL")
    return 1 if failures else 0


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    dsn = os.environ["STABILIZE_PROBE_DSN"]
    phase = sys.argv[sys.argv.index("--phase") + 1]
    return phase_old(dsn) if phase == "old" else phase_new(dsn)


if __name__ == "__main__":
    sys.exit(main())
