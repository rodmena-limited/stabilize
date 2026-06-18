"""Tests for workflow crash recovery and the opt-in processor recovery sweeper.

Characterization tests lock in the existing WorkflowRecovery behavior (which was
thinly covered) and exercise the new additions:
- PostgreSQL ``get_all_pending_workflows`` parity with SQLite.
- The opt-in ``recover_on_start`` / periodic recovery sweeper on QueueProcessor.

All recovery tests run on both backends via the parametrized fixtures.
"""

from __future__ import annotations

import time
from unittest.mock import patch

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowCriteria, WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.recovery import WorkflowRecovery, recover_on_startup

APP = "recovery-test-app"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _make_workflow(name: str = "wf", with_requisites: bool = False) -> Workflow:
    stages = [
        StageExecution(
            ref_id="s1",
            type="test",
            name="Stage 1",
            tasks=[
                TaskExecution.create(
                    name="Task 1",
                    implementing_class="success",
                    stage_start=True,
                    stage_end=True,
                ),
            ],
        ),
    ]
    return Workflow.create(application=APP, name=name, stages=stages)


def _store_running_with_running_task(repository: WorkflowStore) -> Workflow:
    """Persist a workflow that 'crashed' with a RUNNING stage and RUNNING task."""
    wf = _make_workflow()
    wf.status = WorkflowStatus.RUNNING
    wf.start_time = _now_ms()
    repository.store(wf)

    stage = repository.retrieve_stage(wf.stages[0].id)
    stage.status = WorkflowStatus.RUNNING
    stage.start_time = _now_ms()
    stage.tasks[0].status = WorkflowStatus.RUNNING
    stage.tasks[0].start_time = _now_ms()
    repository.store_stage(stage)
    return wf


class TestGetAllPendingWorkflows:
    """Backend parity: get_all_pending_workflows works on SQLite and Postgres."""

    def test_returns_running_and_not_started(self, repository: WorkflowStore, backend: str) -> None:
        running = _make_workflow("running")
        running.status = WorkflowStatus.RUNNING
        running.start_time = _now_ms()
        repository.store(running)

        not_started = _make_workflow("not-started")
        repository.store(not_started)  # default NOT_STARTED

        done = _make_workflow("done")
        done.status = WorkflowStatus.SUCCEEDED
        done.start_time = _now_ms()
        repository.store(done)

        criteria = WorkflowCriteria(
            statuses={WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED},
            page_size=100,
        )
        ids = {wf.id for wf in repository.get_all_pending_workflows(criteria)}

        assert running.id in ids
        assert not_started.id in ids
        assert done.id not in ids


class TestWorkflowRecovery:
    """Characterization tests for WorkflowRecovery."""

    def test_recovers_running_workflow(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        wf = _store_running_with_running_task(repository)

        recovery = WorkflowRecovery(repository, queue)
        results = recovery.recover_pending_workflows(application=APP)

        assert len(results) == 1
        assert results[0].workflow_id == wf.id
        assert results[0].status == "recovered"
        assert results[0].stages_requeued >= 1
        assert queue.size() >= 1

    def test_recovers_not_started_ready_stage(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        wf = _make_workflow("ns")
        repository.store(wf)  # NOT_STARTED, no start_time

        recovery = WorkflowRecovery(repository, queue)
        results = recovery.recover_pending_workflows(application=APP)

        assert len(results) == 1
        assert results[0].status == "recovered"
        assert queue.size() >= 1

    def test_completed_workflow_not_recovered(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        wf = _make_workflow("done")
        wf.status = WorkflowStatus.SUCCEEDED
        wf.start_time = _now_ms()
        repository.store(wf)

        recovery = WorkflowRecovery(repository, queue)
        results = recovery.recover_pending_workflows(application=APP)

        # SUCCEEDED workflows are filtered out by the status query.
        assert all(r.workflow_id != wf.id for r in results)
        assert queue.size() == 0

    def test_recovery_is_idempotent_for_running_task(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        _store_running_with_running_task(repository)
        recovery = WorkflowRecovery(repository, queue)

        recovery.recover_pending_workflows(application=APP)
        size_after_first = queue.size()
        assert size_after_first >= 1

        # Second sweep: the task already has a pending message, so no duplicate.
        recovery.recover_pending_workflows(application=APP)
        assert queue.size() == size_after_first

    def test_old_workflow_skipped_by_age_cutoff(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        # The age cutoff is enforced on the cross-application recovery path
        # (get_all_pending_workflows), which is what the no-application sweeper
        # uses. (The application-filtered path does not currently apply it.)
        old = _make_workflow("old")
        old.status = WorkflowStatus.RUNNING
        old.start_time = _now_ms() - int(100 * 3600 * 1000)  # 100h ago
        repository.store(old)

        recent = _store_running_with_running_task(repository)  # started "now"

        recovery = WorkflowRecovery(repository, queue, max_recovery_age_hours=1.0)
        results = recovery.recover_pending_workflows()  # no app filter -> honors cutoff
        ids = {r.workflow_id for r in results}

        assert old.id not in ids  # too old
        assert recent.id in ids  # within window

    def test_recover_on_startup_helper(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        _store_running_with_running_task(repository)
        results = recover_on_startup(repository, queue, application=APP)
        assert any(r.status == "recovered" for r in results)


class TestProcessorRecoverySweeper:
    """Tests for the opt-in QueueProcessor recovery integration."""

    def test_run_recovery_requeues(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        _store_running_with_running_task(repository)
        processor = QueueProcessor(queue, store=repository)
        results = processor.run_recovery()
        assert any(r.status == "recovered" for r in results)
        assert queue.size() >= 1

    def test_run_recovery_without_store_is_safe(self, queue: Queue, backend: str) -> None:
        processor = QueueProcessor(queue)  # no store
        assert processor.run_recovery() == []

    def test_recover_on_start_invokes_recovery(self, repository: WorkflowStore, queue: Queue, backend: str) -> None:
        config = QueueProcessorConfig(recover_on_start=True)
        processor = QueueProcessor(queue, config=config, store=repository)
        with patch.object(processor, "run_recovery", wraps=processor.run_recovery) as spy:
            processor.start()
            try:
                assert spy.call_count == 1
            finally:
                processor.stop()

    def test_recover_on_start_disabled_by_default(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        processor = QueueProcessor(queue, store=repository)
        with patch.object(processor, "run_recovery") as spy:
            processor.start()
            try:
                assert spy.call_count == 0
            finally:
                processor.stop()

    def test_periodic_recovery_thread_only_when_enabled(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        # Disabled by default -> no recovery thread.
        p1 = QueueProcessor(queue, store=repository)
        p1.start()
        try:
            assert p1._recovery_thread is None
        finally:
            p1.stop()

        # Enabled -> a daemon recovery thread is started.
        config = QueueProcessorConfig(recovery_interval_seconds=10.0)
        p2 = QueueProcessor(queue, config=config, store=repository)
        p2.start()
        try:
            assert p2._recovery_thread is not None
            assert p2._recovery_thread.is_alive()
        finally:
            p2.stop()
