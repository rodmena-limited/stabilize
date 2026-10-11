from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import timedelta

import pytest

from stabilize import StageExecution, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.config import reset_handler_config
from stabilize.tasks.interface import RetryableTask, Task
from tests.audit_support import drive, make_processor, stage, start

FIRST: dict[str, float] = {}


class Fast(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class Slow(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        first = FIRST.setdefault(stage.id, time.monotonic())
        if time.monotonic() - first < 2.0:
            return TaskResult.running()
        return TaskResult.success()


@pytest.fixture
def short_wait_budget(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    monkeypatch.setenv("STABILIZE_MAX_STAGE_WAIT_RETRIES", "3")
    monkeypatch.setenv("STABILIZE_HANDLER_RETRY_DELAY_S", "0.2")
    reset_handler_config()
    yield
    reset_handler_config()


def test_running_branch_is_not_failed_by_the_wait_budget(
    repository: WorkflowStore, queue: Queue, short_wait_budget: None
) -> None:
    FIRST.clear()
    processor = make_processor(
        repository, queue, {"fast": Fast, "slow": Slow}, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100
    )
    wf = start(repository, queue, [stage("fast", ["fast"]), stage("slow", ["slow"])])
    result = drive(processor, repository, wf.id, timeout=15, idle=2.0)
    slow = next(s for s in result.stages if s.ref_id == "slow")
    detail = f"C9 workflow {result.status.name} while a branch ran, slow={slow.status.name}"
    assert result.status == WorkflowStatus.SUCCEEDED, detail
    assert slow.status == WorkflowStatus.SUCCEEDED


def test_control_single_branch_succeeds(repository: WorkflowStore, queue: Queue, short_wait_budget: None) -> None:
    FIRST.clear()
    processor = make_processor(
        repository, queue, {"slow": Slow}, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100
    )
    wf = start(repository, queue, [stage("slow", ["slow"])])
    result = drive(processor, repository, wf.id, timeout=15, idle=2.0)
    assert result.status == WorkflowStatus.SUCCEEDED
