from __future__ import annotations

import time

import pytest

from stabilize import (
    Orchestrator,
    QueueProcessor,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.errors import TransientError
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.config import HandlerConfig

CALLS: dict[str, int] = {}


class AlwaysTransient(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        CALLS[stage.execution.id] = CALLS.get(stage.execution.id, 0) + 1
        raise TransientError("upstream unavailable")


class PollWithBlips(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        n = CALLS[stage.execution.id] = CALLS.get(stage.execution.id, 0) + 1
        if n >= 40:
            return TaskResult.success()
        if n % 4 == 0:
            raise TransientError("blip")
        return TaskResult.running()


def _run(repository: WorkflowStore, queue: Queue, task_name: str, budget: float = 30.0) -> Workflow:
    registry = TaskRegistry()
    registry.register("always_transient", AlwaysTransient)
    registry.register("poll_with_blips", PollWithBlips)
    cfg = HandlerConfig(task_backoff_min_delay_ms=5, task_backoff_max_delay_ms=10)
    processor = QueueProcessor(queue, store=repository, task_registry=registry, handler_config=cfg)
    wf = Workflow.create(
        application="test",
        name="retry-bound",
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
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        processor.process_all(timeout=1.0)
        if repository.retrieve(wf.id).status.is_complete:
            break
    return repository.retrieve(wf.id)


@pytest.fixture(autouse=True)
def _no_breaker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")


def test_always_transient_task_stops_at_max_attempts(repository: WorkflowStore, queue: Queue) -> None:
    result = _run(repository, queue, "always_transient")
    assert result.status == WorkflowStatus.TERMINAL
    assert CALLS[result.id] == 10


def test_running_poll_resets_consecutive_transient_count(repository: WorkflowStore, queue: Queue) -> None:
    result = _run(repository, queue, "poll_with_blips")
    assert result.status == WorkflowStatus.SUCCEEDED
    assert CALLS[result.id] == 40
