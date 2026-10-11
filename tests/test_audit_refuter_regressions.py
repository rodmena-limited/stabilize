from __future__ import annotations

import time
import uuid
from datetime import timedelta

import pytest

from stabilize import Orchestrator, StageExecution, TaskResult, Workflow
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import CancelStage
from stabilize.tasks.interface import RetryableTask, SkippableTask, Task
from tests.audit_support import drive, make_processor, stage, start

POLLS: dict[str, int] = {}


class Slow(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        n = POLLS[stage.id] = POLLS.get(stage.id, 0) + 1
        if n < 5:
            return TaskResult.running()
        return TaskResult.success()


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class GateRaises(SkippableTask):
    def is_enabled(self, stage: StageExecution) -> bool:
        raise RuntimeError("flag service unreachable")

    def do_execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    POLLS.clear()


def test_canceling_a_sibling_does_not_cancel_a_running_branch(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(
        repository, queue, {"slow": Slow, "ok": Ok}, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100
    )
    wf = start(repository, queue, [stage("running", ["slow"]), stage("other", ["ok"], needs={"running"})])
    drive(processor, repository, wf.id, timeout=0.2, idle=0.1)
    before = repository.retrieve(wf.id)
    sibling = next(s for s in before.stages if s.ref_id == "other")
    assert next(s for s in before.stages if s.ref_id == "running").status == WorkflowStatus.RUNNING
    assert sibling.status == WorkflowStatus.NOT_STARTED
    queue.push(CancelStage(execution_type=wf.type.value, execution_id=wf.id, stage_id=sibling.id))
    result = drive(processor, repository, wf.id, timeout=15, idle=1.5)
    running = next(s for s in result.stages if s.ref_id == "running")
    assert running.status == WorkflowStatus.SUCCEEDED, f"F1 running branch ended {running.status.name}"
    assert next(s for s in result.stages if s.ref_id == "other").status == WorkflowStatus.CANCELED
    assert POLLS.get(running.id) == 5


def test_free_slot_with_an_older_waiting_workflow_still_starts_work(repository: WorkflowStore, queue: Queue) -> None:
    cfg = f"cfg-{uuid.uuid4().hex[:8]}"
    processor = make_processor(repository, queue, {"ok": Ok})

    def limited() -> Workflow:
        w = Workflow.create(application="test", name="limited", stages=[stage("s", ["ok"])])
        w.pipeline_config_id = cfg
        w.is_limit_concurrent = True
        w.max_concurrent_executions = 1
        w.keep_waiting_pipelines = True
        return w

    orphan = limited()
    orphan.status = WorkflowStatus.BUFFERED
    repository.store(orphan)
    time.sleep(0.01)
    fresh = limited()
    repository.store(fresh)
    Orchestrator(queue, store=repository).start(fresh)
    for _ in range(3):
        drive(processor, repository, fresh.id, timeout=5, idle=0.5)
        drive(processor, repository, orphan.id, timeout=5, idle=0.5)
    assert repository.retrieve(orphan.id).status == WorkflowStatus.SUCCEEDED, "F4 the older buffered workflow never ran"
    assert repository.retrieve(fresh.id).status == WorkflowStatus.SUCCEEDED, "F4 the new workflow never ran"


def test_gate_error_respects_continue_pipeline_on_failure(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(repository, queue, {"gate": GateRaises, "ok": Ok})
    wf = start(
        repository,
        queue,
        [stage("s", ["gate"], {"continuePipelineOnFailure": True}), stage("after", ["ok"], needs={"s"})],
    )
    result = drive(processor, repository, wf.id, idle=1.5)
    s = next(x for x in result.stages if x.ref_id == "s")
    after = next(x for x in result.stages if x.ref_id == "after")
    assert s.status == WorkflowStatus.FAILED_CONTINUE, f"F9 stage {s.status.name}"
    assert after.status == WorkflowStatus.SUCCEEDED
