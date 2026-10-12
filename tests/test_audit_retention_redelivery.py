from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

import pytest

from stabilize import StageExecution, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import get_message_type_name
from stabilize.tasks.interface import Task
from tests.audit_support import make_processor, stage, start

CALLS: dict[str, int] = {}

TYPES = ["StartWorkflow", "StartStage", "StartTask", "RunTask", "CompleteTask", "CompleteStage", "CompleteWorkflow"]


class Effect(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        CALLS[stage.execution.id] = CALLS.get(stage.execution.id, 0) + 1
        return TaskResult.success()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    CALLS.clear()


def _drain(processor: Any, repository: WorkflowStore, wf_id: str, crash_on: str | None) -> bool:
    crashed = False
    deadline = time.monotonic() + 15
    idle_since = time.monotonic()
    while time.monotonic() < deadline:
        msg = processor.queue.poll_one()
        if msg is None:
            if repository.retrieve(wf_id).status.is_complete or time.monotonic() - idle_since > 3:
                break
            time.sleep(0.05)
            continue
        idle_since = time.monotonic()
        processor._handle_message(msg)
        if crash_on is not None and not crashed and get_message_type_name(msg) == crash_on:
            return True
        processor.queue.ack(msg)
    return crashed


def _run(repository: WorkflowStore, queue: Queue, crash_on: str, sweep: bool) -> tuple[WorkflowStatus, int]:
    queue.lock_duration = timedelta(seconds=1)
    first = make_processor(repository, queue, {"effect": Effect})
    wf = start(repository, queue, [stage("s", ["effect"])])
    assert _drain(first, repository, wf.id, crash_on), f"{crash_on} never handled"
    time.sleep(0.05)
    if sweep:
        repository.cleanup_old_processed_messages(max_age_hours=0)
    time.sleep(1.2)
    second = make_processor(repository, queue, {"effect": Effect})
    _drain(second, repository, wf.id, None)
    return repository.retrieve(wf.id).status, CALLS.get(wf.id, 0)


@pytest.mark.parametrize("crash_on", TYPES)
def test_redelivery_after_retention_does_not_repeat_work(
    repository: WorkflowStore, queue: Queue, crash_on: str
) -> None:
    status, calls = _run(repository, queue, crash_on, sweep=True)
    assert status == WorkflowStatus.SUCCEEDED, f"C2 {crash_on} redelivered after retention: workflow {status.name}"
    assert calls == 1, f"C2 {crash_on} redelivered after retention: task executed {calls} times"


@pytest.mark.parametrize("crash_on", TYPES)
def test_redelivery_control_without_retention(repository: WorkflowStore, queue: Queue, crash_on: str) -> None:
    status, calls = _run(repository, queue, crash_on, sweep=False)
    assert status == WorkflowStatus.SUCCEEDED
    assert calls == 1
