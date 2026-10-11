from __future__ import annotations

import time
from collections.abc import Callable
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
from stabilize.queue.messages import get_message_type_name
from stabilize.resilience.config import HandlerConfig


def make_processor(repository: Any, queue: Any, tasks: dict[str, Any], **handler_kwargs: Any) -> QueueProcessor:
    registry = TaskRegistry()
    for name, impl in tasks.items():
        registry.register(name, impl)
    cfg = HandlerConfig(**handler_kwargs) if handler_kwargs else None
    processor = QueueProcessor(queue, store=repository, task_registry=registry, handler_config=cfg)
    processor.config.retry_delay = timedelta(milliseconds=50)
    return processor


def stage(
    ref: str,
    impls: list[str],
    context: dict[str, Any] | None = None,
    needs: set[str] | None = None,
) -> StageExecution:
    n = len(impls)
    return StageExecution(
        ref_id=ref,
        type="test",
        name=ref,
        context=dict(context or {}),
        requisite_stage_ref_ids=set(needs or set()),
        tasks=[
            TaskExecution.create(f"{ref}-{i}-{impl}", impl, stage_start=i == 0, stage_end=i == n - 1)
            for i, impl in enumerate(impls)
        ],
    )


def start(repository: Any, queue: Any, stages: list[StageExecution], application: str = "test") -> Workflow:
    wf = Workflow.create(application=application, name="audit", stages=stages)
    repository.store(wf)
    Orchestrator(queue, store=repository).start(wf)
    return wf


def drive(
    processor: QueueProcessor,
    repository: Any,
    workflow_id: str,
    timeout: float = 10.0,
    idle: float = 1.0,
    drop: Callable[[Any], bool] | None = None,
) -> Workflow:
    deadline = time.monotonic() + timeout
    last = time.monotonic()
    while time.monotonic() < deadline:
        msg = processor.queue.poll_one()
        if msg is None:
            if repository.retrieve(workflow_id).status.is_complete or time.monotonic() - last > idle:
                break
            time.sleep(0.02)
            continue
        last = time.monotonic()
        if drop is not None and drop(msg):
            processor.queue.ack(msg)
            continue
        try:
            processor._handle_message(msg)
            processor.queue.ack(msg)
        except Exception as exc:
            msg.set_error_context(exc)
            processor.queue.reschedule(msg, processor.config.retry_delay)
    return repository.retrieve(workflow_id)


def drop_first(type_name: str) -> Callable[[Any], bool]:
    state = {"done": False}

    def drop(msg: Any) -> bool:
        if not state["done"] and get_message_type_name(msg) == type_name:
            state["done"] = True
            return True
        return False

    return drop
