from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

import pytest

from stabilize import (
    Orchestrator,
    QueueProcessor,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.errors import TransientError
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.config import HandlerConfig
from stabilize.tasks.interface import RetryableTask, Task

STATE: dict[str, Any] = {}


def _count() -> None:
    STATE["calls"] += 1
    if STATE["calls"] == 1:
        STATE["arm"] = True


class Poller(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(seconds=5)

    def execute(self, stage: StageExecution) -> TaskResult:
        _count()
        return TaskResult.running()


class Flaky(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        _count()
        raise TransientError("blip")


@pytest.mark.parametrize("task_name", ["poller", "flaky"])
def test_failed_processed_mark_after_requeue_does_not_fork(
    repository: WorkflowStore, queue: Queue, task_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    STATE.update(calls=0, arm=False)
    original = repository.mark_message_processed

    def failing_once(message_id: str, handler_type: str | None = None, execution_id: str | None = None) -> None:
        if STATE["arm"] and handler_type == "RunTask":
            STATE["arm"] = False
            raise ConnectionError("server closed the connection unexpectedly")
        original(message_id=message_id, handler_type=handler_type, execution_id=execution_id)

    monkeypatch.setattr(repository, "mark_message_processed", failing_once)
    registry = TaskRegistry()
    registry.register("poller", Poller)
    registry.register("flaky", Flaky)
    cfg = HandlerConfig(task_backoff_min_delay_ms=5000, task_backoff_max_delay_ms=5000)
    processor = QueueProcessor(queue, store=repository, task_registry=registry, handler_config=cfg)
    processor.config.retry_delay = timedelta(milliseconds=50)
    wf = Workflow.create(
        application="test",
        name="requeue",
        stages=[
            StageExecution(
                ref_id="s",
                type="test",
                name="s",
                tasks=[TaskExecution.create("t", task_name, stage_start=True, stage_end=True)],
            )
        ],
    )
    repository.store(wf)
    Orchestrator(queue, store=repository).start(wf)
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        try:
            processor.process_one()
        except Exception:
            pass
        time.sleep(0.02)
    assert STATE["calls"] == 1
    assert queue.size() == 1
