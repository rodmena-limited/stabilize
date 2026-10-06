from __future__ import annotations

import time
from datetime import timedelta
from pathlib import Path

from stabilize import StageExecution, TaskExecution, Workflow, WorkflowStatus
from stabilize.persistence.store import WorkflowCriteria, WorkflowStore
from stabilize.queue import Queue
from stabilize.recovery import WorkflowRecovery


def _wf(app: str) -> Workflow:
    return Workflow.create(
        application=app,
        name="recovery",
        stages=[
            StageExecution(
                ref_id="s",
                type="test",
                name="s",
                tasks=[TaskExecution.create("t", "success", stage_start=True, stage_end=True)],
            )
        ],
    )


def _seed(repository: WorkflowStore, app: str, n: int) -> tuple[set[str], str]:
    ids = set()
    for _ in range(n):
        wf = _wf(app)
        repository.store(wf)
        ids.add(wf.id)
    old = _wf(app)
    old.status = WorkflowStatus.RUNNING
    old.start_time = int(time.time() * 1000) - 48 * 3600 * 1000
    repository.store(old)
    return ids, old.id


def test_recovery_examines_beyond_batch_size(repository: WorkflowStore, queue: Queue) -> None:
    ids, old_id = _seed(repository, "rec-all", 15)
    results = WorkflowRecovery(repository, queue, batch_size=5).recover_pending_workflows()
    recovered = {r.workflow_id for r in results if r.status == "recovered"}
    assert ids <= recovered
    assert old_id not in {r.workflow_id for r in results}


def test_application_recovery_respects_window(repository: WorkflowStore, queue: Queue) -> None:
    ids, old_id = _seed(repository, "rec-app", 15)
    results = WorkflowRecovery(repository, queue, batch_size=5).recover_pending_workflows(application="rec-app")
    recovered = {r.workflow_id for r in results if r.status == "recovered"}
    assert ids <= recovered
    assert old_id not in {r.workflow_id for r in results}


def test_retrieve_by_application_honours_start_time_window(repository: WorkflowStore) -> None:
    ids, old_id = _seed(repository, "rec-crit", 3)
    cutoff = int(time.time() * 1000) - 3600 * 1000
    after = {w.id for w in repository.retrieve_by_application("rec-crit", WorkflowCriteria(start_time_after=cutoff))}
    assert ids <= after and old_id not in after
    before = {w.id for w in repository.retrieve_by_application("rec-crit", WorkflowCriteria(start_time_before=cutoff))}
    assert old_id in before


def test_not_started_workflow_is_recovered_through_start_workflow_once(repository: WorkflowStore, queue: Queue) -> None:
    from stabilize.queue.messages import StartWorkflow

    wf = _wf("rec-ns")
    repository.store(wf)
    first = WorkflowRecovery(repository, queue).recover_pending_workflows(application="rec-ns")
    assert [r.status for r in first] == ["recovered"]
    message = queue.poll_one()
    assert isinstance(message, StartWorkflow) and message.execution_id == wf.id
    queue.reschedule(message, timedelta(seconds=0))
    second = WorkflowRecovery(repository, queue).recover_pending_workflows(application="rec-ns")
    assert [r.status for r in second] == ["skipped"]
    assert queue.size() == 1


def test_purge_skips_a_missing_dead_letter_table(tmp_path: Path) -> None:
    from stabilize import SqliteQueue, SqliteWorkflowStore

    url = f"sqlite:///{tmp_path}/p.db"
    SqliteWorkflowStore(url, create_tables=True)
    custom = SqliteQueue(url, table_name="queue_messages")
    assert custom.purge_workflow("nobody") == 0
    other = SqliteQueue(url, table_name="custom_queue")
    assert other.purge_workflow("nobody") == 0
