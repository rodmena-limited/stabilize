"""Deterministic engine driver for the 1.0.0 probes.

A processor built on a (store, queue) pair from _backends, stepped with
process_one() on the calling thread so a probe's outcome does not depend on
thread scheduling. Handler exceptions are swallowed and counted: process_one
has already rescheduled the message, which is the processor's own behaviour.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

from stabilize import (
    Orchestrator,
    QueueProcessor,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)
from stabilize.resilience.config import HandlerConfig


def processor(store: Any, queue: Any, registry: TaskRegistry, **handler_kwargs: Any) -> QueueProcessor:
    cfg = HandlerConfig(**handler_kwargs) if handler_kwargs else None
    proc = QueueProcessor(queue, store=store, task_registry=registry, handler_config=cfg)
    proc.config.retry_delay = timedelta(milliseconds=50)
    return proc


def tasks(names: list[str]) -> list[TaskExecution]:
    n = len(names)
    return [
        TaskExecution.create(f"{name}-{i}", name, stage_start=i == 0, stage_end=i == n - 1)
        for i, name in enumerate(names)
    ]


def stage(ref: str, names: list[str], context: dict[str, Any] | None = None, needs: set[str] | None = None) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type="probe",
        name=ref,
        context=dict(context or {}),
        requisite_stage_ref_ids=set(needs or set()),
        tasks=tasks(names),
    )


def workflow(stages: list[StageExecution], application: str = "probe") -> Workflow:
    return Workflow.create(application=application, name="probe", stages=stages)


def start(store: Any, queue: Any, wf: Workflow) -> str:
    store.store(wf)
    Orchestrator(queue, store=store).start(wf)
    return wf.id


def drive(proc: QueueProcessor, store: Any, workflow_id: str, timeout: float = 10.0, idle: float = 1.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_work = time.monotonic()
    errors = 0
    while time.monotonic() < deadline:
        try:
            worked = proc.process_one()
        except Exception:
            errors += 1
            worked = True
        if worked:
            last_work = time.monotonic()
        else:
            wf = store.retrieve(workflow_id)
            if wf.status.is_complete or time.monotonic() - last_work > idle:
                break
            time.sleep(0.02)
    wf = store.retrieve(workflow_id)
    return {"workflow": wf, "errors": errors}


def snapshot(wf: Workflow) -> str:
    parts = []
    for s in wf.stages:
        parts.append(f"{s.ref_id}={s.status.name}[" + ",".join(f"{t.name}:{t.status.name}" for t in s.tasks) + "]")
    return f"{wf.status.name} " + " ".join(parts)
