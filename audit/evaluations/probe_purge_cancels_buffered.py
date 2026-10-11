"""keep_waiting_pipelines=False cancels the excess buffered workflows: they end CANCELED.

With is_limit_concurrent, limit 1 and keep_waiting_pipelines=False, when the
running workflow completes StartWaitingWorkflows starts the oldest buffered one
and cancels the rest. A canceled workflow must reach status CANCELED: a
workflow left BUFFERED with is_canceled set is neither running nor finished,
and every recovery sweep that handles BUFFERED work would pick it up again.

  A  W1 running, W2 and W3 buffered; W1 completes: W2 SUCCEEDED, W3 CANCELED
  B  CONTROL keep_waiting_pipelines=True: W2 and W3 both SUCCEEDED

    python audit/evaluations/probe_purge_cancels_buffered.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import timedelta
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402

from stabilize import Orchestrator, QueueProcessor, StageExecution, TaskExecution, TaskRegistry, TaskResult, Workflow  # noqa: E402
from stabilize.resilience.config import HandlerConfig  # noqa: E402
from stabilize.tasks.interface import RetryableTask  # noqa: E402


class Hold(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        polls = int(stage.context.get("polls", 0)) + 1
        if polls < 3:
            return TaskResult.running(context={"polls": polls})
        return TaskResult.success()


def _wf(name: str, cfg: str, keep: bool) -> Workflow:
    wf = Workflow.create(
        application="probe",
        name=name,
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("hold", "hold", stage_start=True, stage_end=True)],
            )
        ],
    )
    wf.pipeline_config_id = cfg
    wf.is_limit_concurrent = True
    wf.max_concurrent_executions = 1
    wf.keep_waiting_pipelines = keep
    return wf


def main() -> int:
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        for label, keep in (("A keep_waiting_pipelines=False", False), ("B CONTROL keep_waiting_pipelines=True", True)):
            registry = TaskRegistry()
            registry.register("hold", Hold)
            with factory() as (store, queue):
                proc = QueueProcessor(
                    queue,
                    store=store,
                    task_registry=registry,
                    handler_config=HandlerConfig(task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100),
                )
                proc.config.retry_delay = timedelta(milliseconds=50)
                cfg = f"cfg-purge-{time.time_ns()}"
                wfs = [_wf(f"w{i}", cfg, keep) for i in range(3)]
                orch = Orchestrator(queue, store=store)
                for wf in wfs:
                    store.store(wf)
                    orch.start(wf)
                deadline = time.monotonic() + 15
                idle = time.monotonic()
                while time.monotonic() < deadline:
                    try:
                        worked = proc.process_one()
                    except Exception:
                        worked = True
                    if worked:
                        idle = time.monotonic()
                    elif time.monotonic() - idle > 1.5:
                        break
                    else:
                        time.sleep(0.02)
                statuses = [store.retrieve(w.id).status.name for w in wfs]
            want = ["SUCCEEDED", "SUCCEEDED", "CANCELED"] if not keep else ["SUCCEEDED"] * 3
            ok = statuses == want
            print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}: statuses={statuses} (want {want})")
            if not ok:
                failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — purged buffered workflows end CANCELED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
