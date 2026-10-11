from __future__ import annotations

from datetime import timedelta

import pytest
from audit_support import drive, make_processor, stage, start

from stabilize import StageExecution, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import RetryableTask, SkippableTask

CALLS = {"enabled": 0, "execute": 0}


class Off(SkippableTask):
    def is_enabled(self, stage: StageExecution) -> bool:
        return False

    def do_execute(self, stage: StageExecution) -> TaskResult:
        CALLS["execute"] += 1
        return TaskResult.success()


class On(SkippableTask):
    def do_execute(self, stage: StageExecution) -> TaskResult:
        CALLS["execute"] += 1
        return TaskResult.success()


class Gate(SkippableTask, RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=50)

    def is_enabled(self, stage: StageExecution) -> bool:
        CALLS["enabled"] += 1
        return bool(stage.context.get("enabled", True))

    def do_execute(self, stage: StageExecution) -> TaskResult:
        CALLS["execute"] += 1
        polls = int(stage.context.get("polls", 0)) + 1
        if polls < 3:
            return TaskResult.running(context={"polls": polls})
        return TaskResult.success()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    CALLS.update(enabled=0, execute=0)


@pytest.mark.parametrize("order", [["on", "off"], ["off", "on"]], ids=["skip-last", "skip-first"])
def test_start_task_skip_completes_the_stage(repository: WorkflowStore, queue: Queue, order: list[str]) -> None:
    processor = make_processor(repository, queue, {"on": On, "off": Off})
    wf = start(repository, queue, [stage("s", order)])
    result = drive(processor, repository, wf.id)
    detail = f"C8 stage wedged after a StartTask skip: {result.stages[0].status.name}"
    assert result.status == WorkflowStatus.SUCCEEDED, detail
    assert CALLS["execute"] == 1


def test_skip_decided_once_for_a_polling_task(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(
        repository, queue, {"gate": Gate}, task_backoff_min_delay_ms=50, task_backoff_max_delay_ms=50
    )
    wf = start(repository, queue, [stage("s", ["gate"])])
    result = drive(processor, repository, wf.id)
    assert result.status == WorkflowStatus.SUCCEEDED
    assert CALLS["enabled"] == 1, f"C13 is_enabled evaluated {CALLS['enabled']} times for one task"


def test_skip_decided_at_start_task_for_a_dotted_implementing_class(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(repository, queue, {"Gate": Gate})
    wf = start(repository, queue, [stage("s", ["probe.pkg.Gate"], {"enabled": False})])
    result = drive(processor, repository, wf.id)
    task = result.stages[0].tasks[0]
    assert task.status == WorkflowStatus.SKIPPED
    assert task.start_time is None, "C13 dotted implementing_class was started before its skip was decided"
    assert CALLS["execute"] == 0


def test_control_skip_returned_from_execute(repository: WorkflowStore, queue: Queue) -> None:
    class OffAtRun(SkippableTask):
        def do_execute(self, stage: StageExecution) -> TaskResult:
            return TaskResult.skipped()

    processor = make_processor(repository, queue, {"on": On, "offrun": OffAtRun})
    wf = start(repository, queue, [stage("s", ["on", "offrun"])])
    assert drive(processor, repository, wf.id).status == WorkflowStatus.SUCCEEDED
