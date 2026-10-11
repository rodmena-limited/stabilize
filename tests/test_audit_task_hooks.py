from __future__ import annotations

import time
from datetime import timedelta

import pytest
from audit_support import drive, make_processor, stage, start

from stabilize import Orchestrator, StageExecution, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import RetryableTask, SkippableTask

CALLS = {"hook": 0, "cancel": 0, "cleanup": 0, "polls": 0}


class _Polls(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(milliseconds=300)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=50)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()


class TimeoutRaises(_Polls):
    def on_timeout(self, stage: StageExecution) -> TaskResult | None:
        CALLS["hook"] += 1
        raise RuntimeError("cleanup service unreachable")


class EnabledRaises(SkippableTask):
    def is_enabled(self, stage: StageExecution) -> bool:
        CALLS["hook"] += 1
        raise RuntimeError("flag service unreachable")

    def do_execute(self, stage: StageExecution) -> TaskResult:
        CALLS["polls"] += 1
        return TaskResult.success()


class Job(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=500)

    def execute(self, stage: StageExecution) -> TaskResult:
        CALLS["polls"] += 1
        return TaskResult.running()

    def on_cancel(self, stage: StageExecution) -> TaskResult | None:
        CALLS["cancel"] += 1
        return None

    def on_cleanup(self, stage: StageExecution) -> None:
        CALLS["cleanup"] += 1


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    CALLS.update(hook=0, cancel=0, cleanup=0, polls=0)


@pytest.mark.parametrize("impl", ["timeout_raises", "enabled_raises"])
def test_raising_hook_ends_the_task_once(repository: WorkflowStore, queue: Queue, impl: str) -> None:
    processor = make_processor(
        repository,
        queue,
        {"timeout_raises": TimeoutRaises, "enabled_raises": EnabledRaises},
        task_backoff_min_delay_ms=50,
        task_backoff_max_delay_ms=50,
    )
    wf = start(repository, queue, [stage("s", [impl])])
    result = drive(processor, repository, wf.id, timeout=10, idle=1.5)
    assert CALLS["hook"] == 1, f"C11 hook called {CALLS['hook']} times"
    assert result.stages[0].tasks[0].status == WorkflowStatus.TERMINAL
    assert CALLS["polls"] == 0


def test_cancel_runs_on_cancel_and_on_cleanup_once(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(
        repository, queue, {"job": Job}, task_backoff_min_delay_ms=500, task_backoff_max_delay_ms=500
    )
    wf = start(repository, queue, [stage("s", ["job"])])
    deadline = time.monotonic() + 5
    while CALLS["polls"] == 0 and time.monotonic() < deadline:
        msg = processor.queue.poll_one()
        if msg is None:
            time.sleep(0.02)
            continue
        processor._handle_message(msg)
        processor.queue.ack(msg)
    Orchestrator(queue, store=repository).cancel(repository.retrieve(wf.id), "test", "abort")
    result = drive(processor, repository, wf.id, timeout=10, idle=1.5)
    assert result.status == WorkflowStatus.CANCELED
    detail = f"C12 on_cancel={CALLS['cancel']} on_cleanup={CALLS['cleanup']}"
    assert (CALLS["cancel"], CALLS["cleanup"]) == (1, 1), detail
