"""limitConcurrent admits at most N RUNNING workflows, and buffered work always starts.

A workflow with is_limit_concurrent and max_concurrent_executions=N is BUFFERED
when N workflows of its pipeline_config_id are RUNNING, and is started by
StartWaitingWorkflows when one of them completes. Both directions are checked.

  A  block: 8 workflows started at once, limit 2, 8 worker threads, 3 rounds:
     the highest RUNNING count observed is <= 2
  B  release: the same 8 (keep_waiting_pipelines=True) all reach SUCCEEDED
  C  lost wake-up: limit 1, W1 RUNNING and W2 BUFFERED; the StartWaitingWorkflows
     message is lost (consumed unhandled); one recovery sweep: W2 SUCCEEDED
  D  CONTROL for C: the same without the loss: W2 SUCCEEDED
  E  FIFO: limit 1, page size 2, 5 workflows created in order: they run in order

RUNNING counts come from store.retrieve_by_pipeline_config_id.

    python audit/evaluations/probe_concurrency_limit.py
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.models.status import WorkflowStatus  # noqa: E402
from stabilize.persistence.store import WorkflowCriteria  # noqa: E402
from stabilize.queue.messages import StartWaitingWorkflows  # noqa: E402
from stabilize.resilience.config import HandlerConfig  # noqa: E402
from stabilize.tasks.interface import RetryableTask  # noqa: E402

ORDER: list[str] = []
LOCK = threading.Lock()


class Hold(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        first = stage.context.setdefault("first_poll", time.time())
        if stage.context.get("recorded") is None:
            with LOCK:
                ORDER.append(stage.execution.name)
            stage.context["recorded"] = True
        if time.time() - first < float(stage.context.get("hold", 0.5)):
            return TaskResult.running(context={"first_poll": first, "recorded": True})
        return TaskResult.success()


def _registry() -> TaskRegistry:
    r = TaskRegistry()
    r.register("hold", Hold)
    return r


def _wf(name: str, cfg: str, limit: int, keep: bool, hold: float) -> Workflow:
    wf = Workflow.create(
        application="probe",
        name=name,
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                context={"hold": hold},
                tasks=[TaskExecution.create("hold", "hold", stage_start=True, stage_end=True)],
            )
        ],
    )
    wf.pipeline_config_id = cfg
    wf.is_limit_concurrent = True
    wf.max_concurrent_executions = limit
    wf.keep_waiting_pipelines = keep
    return wf


def _running(store: Any, cfg: str) -> int:
    return sum(1 for _ in store.retrieve_by_pipeline_config_id(cfg, WorkflowCriteria(statuses={WorkflowStatus.RUNNING})))


def block_and_release(store: Any, queue: Any, rnd: int) -> tuple[int, int]:
    cfg = f"cfg-limit-{rnd}-{time.time_ns()}"
    proc = QueueProcessor(
        queue,
        store=store,
        task_registry=_registry(),
        handler_config=HandlerConfig(task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100),
    )
    proc.config.max_workers = 8
    proc.config.poll_frequency_ms = 5
    proc.config.retry_delay = timedelta(milliseconds=50)
    wfs = [_wf(f"w{i}", cfg, 2, True, 1.0) for i in range(8)]
    for wf in wfs:
        store.store(wf)
    orch = Orchestrator(queue, store=store)
    peak = 0
    proc.start()
    try:
        for wf in wfs:
            orch.start(wf)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            peak = max(peak, _running(store, cfg))
            if all(store.retrieve(w.id).status.is_complete for w in wfs):
                break
            time.sleep(0.02)
    finally:
        proc.stop(wait=True, timeout=10)
    done = sum(1 for w in wfs if store.retrieve(w.id).status == WorkflowStatus.SUCCEEDED)
    return peak, done


def _step(proc: QueueProcessor, store: Any, ids: list[str], drop_wakeup: bool, seconds: float) -> int:
    dropped = 0
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if all(store.retrieve(i).status.is_complete for i in ids):
            break
        msg = proc.queue.poll_one()
        if msg is None:
            time.sleep(0.02)
            continue
        if drop_wakeup and isinstance(msg, StartWaitingWorkflows):
            proc.queue.ack(msg)
            dropped += 1
            continue
        try:
            proc._handle_message(msg)
            proc.queue.ack(msg)
        except Exception:
            proc.queue.reschedule(msg, timedelta(milliseconds=50))
    return dropped


def lost_wakeup(store: Any, queue: Any, drop: bool) -> tuple[str, int]:
    cfg = f"cfg-wake-{time.time_ns()}"
    proc = QueueProcessor(
        queue,
        store=store,
        task_registry=_registry(),
        handler_config=HandlerConfig(task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100),
    )
    proc.config.recovery_application = "probe"
    w1, w2 = _wf("w1", cfg, 1, True, 0.3), _wf("w2", cfg, 1, True, 0.3)
    orch = Orchestrator(queue, store=store)
    for wf in (w1, w2):
        store.store(wf)
        orch.start(wf)
    dropped = _step(proc, store, [w1.id, w2.id], drop, 6)
    proc.run_recovery()
    _step(proc, store, [w1.id, w2.id], False, 6)
    return store.retrieve(w2.id).status.name, dropped


def fifo(store: Any, queue: Any) -> list[str]:
    cfg = f"cfg-fifo-{time.time_ns()}"
    ORDER.clear()
    proc = QueueProcessor(
        queue,
        store=store,
        task_registry=_registry(),
        handler_config=HandlerConfig(task_backoff_min_delay_ms=50, task_backoff_max_delay_ms=50, default_page_size=2),
    )
    wfs = [_wf(f"f{i}", cfg, 1, True, 0.1) for i in range(5)]
    orch = Orchestrator(queue, store=store)
    for wf in wfs:
        store.store(wf)
        orch.start(wf)
        time.sleep(0.005)
    _step(proc, store, [w.id for w in wfs], False, 20)
    return list(ORDER)


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
        peaks, dones = [], []
        for rnd in range(3):
            with factory() as (store, queue):
                peak, done = block_and_release(store, queue, rnd)
            peaks.append(peak)
            dones.append(done)
        report(max(peaks) <= 2, f"{name} A block", f"peak RUNNING per round={peaks} (want <= 2)")
        report(min(dones) == 8, f"{name} B release", f"SUCCEEDED per round={dones} (want 8)")
        with factory() as (store, queue):
            status, dropped = lost_wakeup(store, queue, True)
        report(status == "SUCCEEDED" and dropped >= 1, f"{name} C lost wake-up", f"W2={status} wake-ups dropped={dropped} (want SUCCEEDED, >=1)")
        with factory() as (store, queue):
            status, _ = lost_wakeup(store, queue, False)
        report(status == "SUCCEEDED", f"{name} D CONTROL wake-up delivered", f"W2={status} (want SUCCEEDED)")
        with factory() as (store, queue):
            order = fifo(store, queue)
        want = [f"f{i}" for i in range(5)]
        report(order == want, f"{name} E FIFO", f"run order={order} (want {want})")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — the limit holds and buffered work starts, oldest first")
    return 0


if __name__ == "__main__":
    sys.exit(main())
