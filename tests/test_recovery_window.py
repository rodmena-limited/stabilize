from __future__ import annotations

import time

import pytest

from stabilize import StageExecution, TaskResult, Workflow, WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.recovery import WorkflowRecovery
from stabilize.tasks.interface import Task
from tests.audit_support import drive, make_processor, stage


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")


def _stranded(repository: WorkflowStore, hours_ago: float) -> Workflow:
    wf = Workflow.create(application="rec-window", name="stranded", stages=[stage("s", ["ok"])])
    wf.status = WorkflowStatus.RUNNING
    wf.start_time = int(time.time() * 1000) - int(hours_ago * 3600 * 1000)
    repository.store(wf)
    return wf


def test_workflow_started_before_the_window_is_skipped(repository: WorkflowStore, queue: Queue) -> None:
    wf = _stranded(repository, 30)
    results = WorkflowRecovery(repository, queue, max_recovery_age_hours=24).recover_pending_workflows()
    assert wf.id not in {r.workflow_id for r in results}, "C30 a workflow started 30 h ago was recovered at 24 h"
    assert queue.size() == 0
    assert repository.retrieve(wf.id).status == WorkflowStatus.RUNNING


def test_raising_the_window_recovers_and_completes_it(repository: WorkflowStore, queue: Queue) -> None:
    wf = _stranded(repository, 30)
    results = WorkflowRecovery(repository, queue, max_recovery_age_hours=72).recover_pending_workflows()
    assert {r.workflow_id: r.status for r in results}.get(wf.id) == "recovered", f"C30 not recovered: {results}"
    processor = make_processor(repository, queue, {"ok": Ok})
    final = drive(processor, repository, wf.id, timeout=15, idle=1.5)
    assert final.status == WorkflowStatus.SUCCEEDED, f"C30 recovered workflow ended {final.status.name}"
