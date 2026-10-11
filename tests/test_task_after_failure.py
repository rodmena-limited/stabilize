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
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task

COUNT = {"side": 0}


class SideEffect(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        COUNT["side"] += 1
        return TaskResult.success()


class Terminal(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("precondition failed")


class Raises(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        raise ValueError("precondition failed")


class Stopped(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.stopped()


class Canceled(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.canceled()


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def _run(repository: WorkflowStore, queue: Queue, first: str, context: dict[str, Any]) -> Workflow:
    registry = TaskRegistry()
    for key, cls in (
        ("terminal", Terminal),
        ("raises", Raises),
        ("stopped", Stopped),
        ("canceled", Canceled),
        ("ok", Ok),
        ("side", SideEffect),
    ):
        registry.register(key, cls)
    processor = QueueProcessor(queue, store=repository, task_registry=registry)
    processor.config.retry_delay = timedelta(milliseconds=50)
    wf = Workflow.create(
        application="test",
        name="task-after-failure",
        stages=[
            StageExecution(
                ref_id="s",
                type="test",
                name="s",
                context=dict(context),
                tasks=[
                    TaskExecution.create("first", first, stage_start=True),
                    TaskExecution.create("side", "side", stage_end=True),
                ],
            )
        ],
    )
    repository.store(wf)
    Orchestrator(queue, store=repository).start(wf)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            worked = processor.process_one()
        except Exception:
            worked = True
        if not worked:
            if repository.retrieve(wf.id).status.is_complete:
                break
            time.sleep(0.02)
    return repository.retrieve(wf.id)


@pytest.mark.parametrize(
    ("first", "context"),
    [
        ("terminal", {}),
        ("raises", {}),
        ("stopped", {}),
        ("canceled", {}),
        ("terminal", {"continuePipelineOnFailure": True}),
    ],
    ids=["terminal", "raises", "stopped", "canceled", "continue-on-failure-terminal"],
)
def test_no_task_starts_after_a_task_fails_its_stage(
    repository: WorkflowStore, queue: Queue, first: str, context: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    COUNT["side"] = 0
    wf = _run(repository, queue, first, context)
    side = next(t for t in wf.stages[0].tasks if t.name == "side")
    assert COUNT["side"] == 0, f"C0 side_effect executed after a {first} task: {COUNT['side']} time(s)"
    assert side.start_time is None
    assert side.status in {WorkflowStatus.NOT_STARTED, WorkflowStatus.SKIPPED}
    assert wf.status.is_complete


def test_continue_on_failure_stage_ends_failed_continue(
    repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    COUNT["side"] = 0
    wf = _run(repository, queue, "terminal", {"continuePipelineOnFailure": True})
    assert wf.stages[0].status == WorkflowStatus.FAILED_CONTINUE


def test_control_next_task_runs_after_success(
    repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    COUNT["side"] = 0
    wf = _run(repository, queue, "ok", {})
    assert COUNT["side"] == 1
    assert wf.status == WorkflowStatus.SUCCEEDED
