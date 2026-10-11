from __future__ import annotations

import time
import uuid
from datetime import timedelta

import pytest

from stabilize import Orchestrator, StageExecution, TaskResult, Workflow
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowCriteria, WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import RetryableTask
from tests.audit_support import drive, drop_first, make_processor, stage


class Hold(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        polls = int(stage.context.get("polls", 0)) + 1
        if polls < int(stage.context.get("hold_polls", 3)):
            return TaskResult.running(context={"polls": polls})
        return TaskResult.success()


def _wf(cfg: str, limit: int, keep: bool, hold_polls: int = 3) -> Workflow:
    wf = Workflow.create(application="test", name="limit", stages=[stage("s", ["hold"], {"hold_polls": hold_polls})])
    wf.pipeline_config_id = cfg
    wf.is_limit_concurrent = True
    wf.max_concurrent_executions = limit
    wf.keep_waiting_pipelines = keep
    return wf


def _running(repository: WorkflowStore, cfg: str) -> int:
    criteria = WorkflowCriteria(statuses={WorkflowStatus.RUNNING})
    return sum(1 for _ in repository.retrieve_by_pipeline_config_id(cfg, criteria))


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")


def test_limit_holds_under_concurrent_starts_and_all_work_completes(repository: WorkflowStore, queue: Queue) -> None:
    peaks, done = [], []
    for _ in range(3):
        cfg = f"cfg-{uuid.uuid4().hex[:8]}"
        processor = make_processor(
            repository, queue, {"hold": Hold}, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100
        )
        processor.config.max_workers = 8
        processor.config.poll_frequency_ms = 5
        wfs = [_wf(cfg, 2, True, hold_polls=8) for _ in range(8)]
        for wf in wfs:
            repository.store(wf)
        peak = 0
        processor.start()
        try:
            for wf in wfs:
                Orchestrator(queue, store=repository).start(wf)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not all(repository.retrieve(w.id).status.is_complete for w in wfs):
                peak = max(peak, _running(repository, cfg))
                time.sleep(0.02)
        finally:
            processor.stop(wait=True, timeout=10)
        peaks.append(peak)
        done.append(sum(1 for w in wfs if repository.retrieve(w.id).status == WorkflowStatus.SUCCEEDED))
    assert max(peaks) <= 2, f"C4 limit 2 exceeded: peak RUNNING per round {peaks}"
    assert done == [8, 8, 8], f"C4 release: SUCCEEDED per round {done}"


def test_recovery_starts_buffered_work_after_a_lost_wake_up(repository: WorkflowStore, queue: Queue) -> None:
    cfg = f"cfg-{uuid.uuid4().hex[:8]}"
    processor = make_processor(
        repository, queue, {"hold": Hold}, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100
    )
    processor.config.recovery_application = "test"
    w1, w2 = _wf(cfg, 1, True), _wf(cfg, 1, True)
    for wf in (w1, w2):
        repository.store(wf)
        Orchestrator(queue, store=repository).start(wf)
    drive(processor, repository, w1.id, timeout=10, idle=1.0, drop=drop_first("StartWaitingWorkflows"))
    processor.run_recovery()
    result = drive(processor, repository, w2.id, timeout=10, idle=1.0)
    assert result.status == WorkflowStatus.SUCCEEDED, f"C5 W2 {result.status.name} after a lost wake-up and a sweep"


def test_purged_buffered_workflow_ends_canceled(repository: WorkflowStore, queue: Queue) -> None:
    cfg = f"cfg-{uuid.uuid4().hex[:8]}"
    processor = make_processor(
        repository, queue, {"hold": Hold}, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100
    )
    wfs = [_wf(cfg, 1, False) for _ in range(3)]
    for wf in wfs:
        repository.store(wf)
        Orchestrator(queue, store=repository).start(wf)
    for wf in wfs:
        drive(processor, repository, wf.id, timeout=10, idle=1.0)
    statuses = [repository.retrieve(w.id).status for w in wfs]
    assert statuses[2] == WorkflowStatus.CANCELED, f"N7 purged workflow left {statuses[2].name}"
