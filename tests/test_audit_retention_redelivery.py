from __future__ import annotations

import subprocess
import sys
import time
from datetime import timedelta
from typing import Any

import pytest

from stabilize import StageExecution, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
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


def _queue(repository: WorkflowStore) -> Any:
    from stabilize.persistence.factory import create_queue

    q = create_queue(repository.connection_string, table_name="queue_messages", lock_duration=timedelta(seconds=1))
    create = getattr(q, "_create_table", None)
    if callable(create):
        create()
    q.clear()
    return q


def _drain(processor: Any, repository: WorkflowStore, wf_id: str, crash_on: str | None) -> bool:
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
        if crash_on is not None and get_message_type_name(msg) == crash_on:
            return True
        processor.queue.ack(msg)
    return False


def _prune(repository: WorkflowStore) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "stabilize.cli.main",
            "prune-processed",
            "--db-url",
            repository.connection_string,
            "--older-than-hours",
            "0",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"prune-processed failed: {result.stderr[-400:]}"


def _run(repository: WorkflowStore, crash_on: str, prune: bool) -> tuple[WorkflowStatus, int]:
    queue = _queue(repository)
    first = make_processor(repository, queue, {"effect": Effect})
    wf = start(repository, queue, [stage("s", ["effect"])])
    assert _drain(first, repository, wf.id, crash_on), f"{crash_on} never handled"
    time.sleep(0.05)
    if prune:
        _prune(repository)
    time.sleep(1.2)
    second = make_processor(repository, queue, {"effect": Effect})
    try:
        _drain(second, repository, wf.id, None)
    finally:
        queue.close()
    return repository.retrieve(wf.id).status, CALLS.get(wf.id, 0)


@pytest.mark.parametrize("crash_on", TYPES)
def test_prune_during_outage_does_not_repeat_work(file_repository: WorkflowStore, crash_on: str) -> None:
    status, calls = _run(file_repository, crash_on, prune=True)
    assert status == WorkflowStatus.SUCCEEDED, f"C2 {crash_on} after prune-processed: workflow {status.name}"
    assert calls == 1, f"C2 {crash_on} redelivered after prune-processed: task executed {calls} times"


@pytest.mark.parametrize("crash_on", TYPES)
def test_redelivery_control_without_prune(file_repository: WorkflowStore, crash_on: str) -> None:
    status, calls = _run(file_repository, crash_on, prune=False)
    assert status == WorkflowStatus.SUCCEEDED
    assert calls == 1
