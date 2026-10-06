from __future__ import annotations

import time
from datetime import timedelta

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
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import RetryableTask


class SlowCall(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(seconds=30)

    def execute(self, stage: StageExecution) -> TaskResult:
        time.sleep(1.0)
        return TaskResult.success(outputs={"finished": True})

    def on_timeout(self, stage: StageExecution) -> TaskResult:
        return TaskResult.failed_continue(error="call timed out", outputs={"timed_out": True})


class SlowCallBoundedPerCall(SlowCall):
    def get_execution_timeout(self, stage: StageExecution) -> timedelta:
        return timedelta(milliseconds=200)


def _run(repository: WorkflowStore, queue: Queue, task_name: str) -> dict:
    registry = TaskRegistry()
    registry.register("slow", SlowCall)
    registry.register("slow_bounded", SlowCallBoundedPerCall)
    wf = Workflow.create(
        application="test",
        name="call-timeout",
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
    QueueProcessor(queue, store=repository, task_registry=registry).process_all(timeout=15.0)
    return repository.retrieve(wf.id).stages[0].outputs


@pytest.fixture(autouse=True)
def _no_breaker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")


def test_per_call_limit_is_separate_from_lifecycle_limit(repository: WorkflowStore, queue: Queue) -> None:
    outputs = _run(repository, queue, "slow_bounded")
    assert outputs.get("timed_out") is True
    assert "finished" not in outputs


def test_default_per_call_limit_is_the_lifecycle_limit(repository: WorkflowStore, queue: Queue) -> None:
    outputs = _run(repository, queue, "slow")
    assert outputs.get("finished") is True
