from __future__ import annotations

import time
import uuid
from datetime import timedelta
from typing import Any

import pytest

from stabilize import Orchestrator, StageExecution, TaskResult, Workflow
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import RetryableTask, Task
from tests.audit_support import drive, make_processor, stage

POLLS: dict[str, int] = {}
RUNS: dict[str, int] = {}


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        RUNS[stage.ref_id] = RUNS.get(stage.ref_id, 0) + 1
        return TaskResult.success()


class Poll(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        n = POLLS[stage.id] = POLLS.get(stage.id, 0) + 1
        return TaskResult.success() if n >= 4 else TaskResult.running()


class JumpOnce(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        RUNS[stage.ref_id] = RUNS.get(stage.ref_id, 0) + 1
        if RUNS[stage.ref_id] == 1:
            return TaskResult.jump_to("slow", context={"jumped": True})
        return TaskResult.success()


TASKS = {"ok": Ok, "poll": Poll, "jump": JumpOnce}


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    POLLS.clear()
    RUNS.clear()


def _processor(repository: WorkflowStore, queue: Queue) -> Any:
    return make_processor(repository, queue, TASKS, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100)


def _until(processor: Any, repository: WorkflowStore, wf_id: str, ok: Any, seconds: float = 15) -> Workflow:
    deadline = time.monotonic() + seconds
    while True:
        wf = drive(processor, repository, wf_id, timeout=0.2, idle=0.1)
        if ok(wf):
            return wf
        assert time.monotonic() < deadline, f"condition not reached: {[(s.ref_id, s.status.name) for s in wf.stages]}"


def _assert_done(wf: Workflow, label: str) -> None:
    states = {s.ref_id: s.status.name for s in wf.stages}
    assert wf.status == WorkflowStatus.SUCCEEDED, f"C18 {label}: workflow {wf.status.name}, stages {states}"
    stuck = [r for r, st in states.items() if st in ("RUNNING", "NOT_STARTED", "PAUSED", "BUFFERED")]
    assert not stuck, f"C18 {label}: stages left {stuck}"


def _start(repository: WorkflowStore, queue: Queue, wf: Workflow) -> Workflow:
    repository.store(wf)
    Orchestrator(queue, store=repository).start(wf)
    return wf


def test_pause_while_a_stage_runs_and_its_successor_has_not_started(repository: WorkflowStore, queue: Queue) -> None:
    processor = _processor(repository, queue)
    wf = _start(
        repository,
        queue,
        Workflow.create(
            application="c18", name="pause-running", stages=[stage("a", ["poll"]), stage("b", ["ok"], needs={"a"})]
        ),
    )
    _until(processor, repository, wf.id, lambda w: w.stages[0].status == WorkflowStatus.RUNNING)
    repository.pause(wf.id, paused_by="audit")
    paused = _until(processor, repository, wf.id, lambda w: w.status == WorkflowStatus.PAUSED, seconds=10)
    assert next(s for s in paused.stages if s.ref_id == "b").status == WorkflowStatus.NOT_STARTED
    started = time.monotonic()
    Orchestrator(queue, store=repository).unpause(paused)
    final = drive(processor, repository, wf.id, timeout=30, idle=2.0)
    _assert_done(final, "pause RUNNING / NOT_STARTED, then resume")
    assert time.monotonic() - started < 30
    assert RUNS.get("b") == 1


def test_pause_before_the_first_message_is_handled(repository: WorkflowStore, queue: Queue) -> None:
    processor = _processor(repository, queue)
    wf = _start(
        repository,
        queue,
        Workflow.create(
            application="c18", name="pause-early", stages=[stage("a", ["ok"]), stage("b", ["ok"], needs={"a"})]
        ),
    )
    repository.pause(wf.id, paused_by="audit")
    drive(processor, repository, wf.id, timeout=3, idle=1.0)
    Orchestrator(queue, store=repository).unpause(repository.retrieve(wf.id))
    final = drive(processor, repository, wf.id, timeout=30, idle=2.0)
    _assert_done(final, "pause before start, then resume")
    assert RUNS == {"a": 1, "b": 1}


def test_pause_a_buffered_workflow_then_resume(repository: WorkflowStore, queue: Queue) -> None:
    processor = _processor(repository, queue)
    cfg = f"cfg-{uuid.uuid4().hex[:8]}"

    def limited(name: str, impl: str) -> Workflow:
        w = Workflow.create(application="c18", name=name, stages=[stage(name, [impl])])
        w.pipeline_config_id = cfg
        w.is_limit_concurrent = True
        w.max_concurrent_executions = 1
        w.keep_waiting_pipelines = True
        return w

    first = _start(repository, queue, limited("first", "poll"))
    _until(processor, repository, first.id, lambda w: w.status == WorkflowStatus.RUNNING)
    second = _start(repository, queue, limited("second", "ok"))
    _until(processor, repository, second.id, lambda w: w.status == WorkflowStatus.BUFFERED)
    repository.pause(second.id, paused_by="audit")
    _assert_done(drive(processor, repository, first.id, timeout=30, idle=2.0), "first of a limited pair")
    Orchestrator(queue, store=repository).unpause(repository.retrieve(second.id))
    for _ in range(10):
        drive(processor, repository, second.id, timeout=3, idle=1.0)
        if repository.retrieve(second.id).status.is_complete:
            break
    _assert_done(repository.retrieve(second.id), "pause BUFFERED, then resume")


def test_jump_into_a_stage_that_is_still_running(repository: WorkflowStore, queue: Queue) -> None:
    processor = _processor(repository, queue)
    wf = _start(
        repository,
        queue,
        Workflow.create(
            application="c18",
            name="jump-running",
            stages=[
                stage("root", ["ok"]),
                stage("slow", ["poll"], needs={"root"}),
                stage("jumper", ["jump"], needs={"root"}),
            ],
        ),
    )
    final = drive(processor, repository, wf.id, timeout=30, idle=2.0)
    assert final.status.is_complete, f"C18 jump into RUNNING: workflow {final.status.name}"
    _assert_done(final, "jump into a RUNNING stage")


def test_pause_of_a_completed_workflow_leaves_it_completed(repository: WorkflowStore, queue: Queue) -> None:
    processor = _processor(repository, queue)
    wf = _start(repository, queue, Workflow.create(application="c18", name="done", stages=[stage("a", ["ok"])]))
    _assert_done(drive(processor, repository, wf.id, timeout=15, idle=1.0), "completed before pause")
    repository.pause(wf.id, paused_by="audit")
    after = repository.retrieve(wf.id)
    assert after.status == WorkflowStatus.SUCCEEDED, f"C18 pause rewrote a SUCCEEDED workflow to {after.status.name}"


def test_cancel_of_a_completed_workflow_leaves_it_uncanceled(repository: WorkflowStore, queue: Queue) -> None:
    processor = _processor(repository, queue)
    wf = _start(repository, queue, Workflow.create(application="c18", name="done", stages=[stage("a", ["ok"])]))
    _assert_done(drive(processor, repository, wf.id, timeout=15, idle=1.0), "completed before cancel")
    repository.cancel(wf.id, canceled_by="audit", reason="late")
    after = repository.retrieve(wf.id)
    assert after.status == WorkflowStatus.SUCCEEDED
    assert not after.is_canceled, "C18 cancel flagged a SUCCEEDED workflow as canceled"
