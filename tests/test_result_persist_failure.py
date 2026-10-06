from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

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
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.config import HandlerConfig

STATE: dict[str, Any] = {}


class Succeeds(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        STATE["calls"] += 1
        STATE["armed"] = STATE["error"] is not None
        return TaskResult.success(outputs={"done": True})


def _inject(store: WorkflowStore) -> None:
    original = store.transaction

    @contextmanager
    def transaction(queue: Queue | None = None) -> Iterator[Any]:
        if STATE["armed"]:
            STATE["armed"] = False
            STATE["fired"] += 1
            raise STATE["error"]
        with original(queue) as txn:
            yield txn

    store.transaction = transaction  # type: ignore[method-assign]


class OperationalError(Exception):
    pass


class DataError(Exception):
    pass


def _run_with_fault(
    repository: WorkflowStore, queue: Queue, error: Exception | None, monkeypatch: pytest.MonkeyPatch
) -> Workflow:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    STATE.update(armed=False, fired=0, calls=0, error=error)
    registry = TaskRegistry()
    registry.register("succeeds", Succeeds)
    cfg = HandlerConfig(task_backoff_min_delay_ms=5, task_backoff_max_delay_ms=10)
    processor = QueueProcessor(queue, store=repository, task_registry=registry, handler_config=cfg)
    processor.config.retry_delay = timedelta(milliseconds=20)
    wf = Workflow.create(
        application="test",
        name="persist-failure",
        stages=[
            StageExecution(
                ref_id="s",
                type="test",
                name="s",
                tasks=[TaskExecution.create("t", "succeeds", stage_start=True, stage_end=True)],
            )
        ],
    )
    repository.store(wf)
    Orchestrator(queue, store=repository).start(wf)
    _inject(repository)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            processor.process_all(timeout=1.0)
        except Exception:
            pass
        if repository.retrieve(wf.id).status.is_complete:
            break
    return repository.retrieve(wf.id)


@pytest.mark.parametrize(
    "error",
    [None, OperationalError("canceling statement due to statement timeout"), TimeoutError("pool exhausted")],
    ids=["control", "store-unavailable", "transient"],
)
def test_failed_save_of_successful_result_neither_fails_nor_reruns(
    repository: WorkflowStore, queue: Queue, error: Exception | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _run_with_fault(repository, queue, error, monkeypatch)
    assert result.status == WorkflowStatus.SUCCEEDED
    assert STATE["calls"] == 1
    assert STATE["fired"] == (0 if error is None else 1)


def test_result_the_store_refuses_ends_terminal_with_the_reason(
    repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _run_with_fault(repository, queue, DataError("unsupported Unicode escape sequence"), monkeypatch)
    assert result.status == WorkflowStatus.TERMINAL
    assert STATE["calls"] == 1
    error = result.stages[0].context["exception"]["details"]["error"]
    assert "could not be stored" in error and "DataError" in error


def test_held_result_is_not_applied_to_a_later_execution_of_the_same_task() -> None:
    from stabilize.handlers.run_task import held_results

    task = TaskExecution.create("t", "succeeds", stage_start=True, stage_end=True)
    task.start_time = 1000
    held_results.hold(task, TaskResult.success())
    task.start_time = 2000
    assert held_results.take(task) is None

    task.start_time = 3000
    held_results.hold(task, TaskResult.success())
    assert held_results.take(task) is not None
    assert held_results.take(task) is None
