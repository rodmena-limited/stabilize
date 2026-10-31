"""WI-5 slice 5a: recovery ages a workflow by its activity, pages its candidates, and reports what it did."""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import RunTask, StartStage
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")

HOUR_MS = 3_600_000


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgrecov") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'recovery.db'}"
        return
    yield request.getfixturevalue("db")


def _parts(url: str) -> tuple[Any, Any]:
    if url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        return SqliteWorkflowStore(url, create_tables=True), SqliteQueue(url)
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    return PostgresWorkflowStore(url), PostgresQueue(url)


def _task(task_type: str = "noop") -> TaskExecution:
    return TaskExecution.create(name="t", implementing_class=task_type, stage_start=True, stage_end=True)


def _stage(ref_id: str, **extra: Any) -> StageExecution:
    extra.setdefault("type", "noop")
    return StageExecution(ref_id=ref_id, name=ref_id.upper(), tasks=[_task()], **extra)


def _running_workflow(store: Any, age_ms: int, queue_table: str = "queue_messages") -> Workflow:
    """A RUNNING workflow whose only stage and task are RUNNING, started *age_ms* ago, with no later activity."""
    now = int(time.time() * 1000)
    wf = Workflow.create(application="recovery", name="w", stages=[_stage("s0")])
    wf.status = WorkflowStatus.RUNNING
    wf.start_time = now - age_ms
    wf.created_at_ms = now - age_ms
    wf.queue_table = queue_table
    stage = wf.stages[0]
    stage.status = WorkflowStatus.RUNNING
    stage.start_time = now - age_ms
    stage.tasks[0].status = WorkflowStatus.RUNNING
    stage.tasks[0].start_time = now - age_ms
    store.store(wf)
    return wf


def test_a_workflow_with_no_activity_inside_the_window_is_stale_not_recovered(any_db: str) -> None:
    from stabilize.recovery import WorkflowRecovery

    store, queue = _parts(any_db)
    try:
        wf = _running_workflow(store, 48 * HOUR_MS)
        results = WorkflowRecovery(store, queue, max_recovery_age_hours=24).recover_pending_workflows()
        by_id = {r.workflow_id: r for r in results}
        assert by_id[wf.id].status == "stale", by_id[wf.id]
        assert queue.size() == 0, "a stale workflow was re-queued"
    finally:
        queue.close()
        store.close()


def test_recent_row_activity_rescues_an_old_workflow(any_db: str) -> None:
    from stabilize.recovery import WorkflowRecovery

    store, queue = _parts(any_db)
    try:
        wf = _running_workflow(store, 30 * HOUR_MS)
        stage = store.retrieve_stage(wf.stages[0].id)
        stage.context["poll"] = 1
        store.store_stage(stage)
        fresh = store.retrieve_stage(stage.id)
        assert fresh.stabilize_updated_at is not None
        assert abs(fresh.stabilize_updated_at - time.time() * 1000) < 60_000
        results = WorkflowRecovery(store, queue, max_recovery_age_hours=24).recover_pending_workflows()
        by_id = {r.workflow_id: r for r in results}
        assert by_id[wf.id].status == "recovered", by_id[wf.id]
        message = queue.poll_one()
        assert isinstance(message, RunTask) and message.task_id == stage.tasks[0].id
    finally:
        queue.close()
        store.close()


def test_the_activity_column_is_null_at_insert_and_set_by_updates(any_db: str) -> None:
    store, queue = _parts(any_db)
    try:
        wf = Workflow.create(application="recovery", name="activity", stages=[_stage("s0")])
        store.store(wf)
        inserted = store.retrieve(wf.id)
        assert inserted.stabilize_updated_at is None
        assert inserted.created_at_ms is not None and abs(inserted.created_at_ms - time.time() * 1000) < 60_000
        stage = store.retrieve_stage(wf.stages[0].id)
        assert stage.stabilize_updated_at is None and stage.tasks[0].stabilize_updated_at is None
        before = int(time.time() * 1000)
        stage.status = WorkflowStatus.RUNNING
        stage.start_time = before
        stage.tasks[0].status = WorkflowStatus.RUNNING
        stage.tasks[0].start_time = before
        store.store_stage(stage)
        inserted.status = WorkflowStatus.RUNNING
        store.update_status(inserted)
        after_stage = store.retrieve_stage(stage.id)
        assert after_stage.stabilize_updated_at is not None and after_stage.stabilize_updated_at >= before - 1000
        assert after_stage.tasks[0].stabilize_updated_at is not None
        assert store.retrieve(wf.id).stabilize_updated_at is not None
    finally:
        queue.close()
        store.close()


def test_recovery_does_not_push_a_start_stage_that_is_already_queued(any_db: str) -> None:
    from stabilize.recovery import WorkflowRecovery

    store, queue = _parts(any_db)
    try:
        now = int(time.time() * 1000)
        wf = Workflow.create(
            application="recovery",
            name="pending",
            stages=[_stage("s0"), _stage("s1", requisite_stage_ref_ids={"s0"}, mutex_key="one")],
        )
        wf.status = WorkflowStatus.RUNNING
        wf.start_time = now
        wf.queue_table = "queue_messages"
        wf.stages[0].status = WorkflowStatus.SUCCEEDED
        wf.stages[0].tasks[0].status = WorkflowStatus.SUCCEEDED
        store.store(wf)
        blocked = wf.stages[1]
        queue.push(StartStage(execution_type="PIPELINE", execution_id=wf.id, stage_id=blocked.id, retry_count=3))
        WorkflowRecovery(store, queue, max_recovery_age_hours=24).recover_pending_workflows()
        WorkflowRecovery(store, queue, max_recovery_age_hours=24).recover_pending_workflows()
        assert queue.size() == 1, "each sweep added another StartStage for the blocked stage"
    finally:
        queue.close()
        store.close()


def test_recovery_candidates_are_paged_and_exclude_complete_workflows(any_db: str) -> None:
    store, queue = _parts(any_db)
    try:
        live = [_running_workflow(store, HOUR_MS) for _ in range(3)]
        done = Workflow.create(application="recovery", name="done", stages=[_stage("s0")])
        done.status = WorkflowStatus.SUCCEEDED
        store.store(done)
        elsewhere = _running_workflow(store, HOUR_MS, queue_table="other_queue")
        first = store.recovery_candidate_ids("queue_messages", after_id=None, limit=2)
        assert len(first) == 2
        second = store.recovery_candidate_ids("queue_messages", after_id=first[-1], limit=2)
        found = set(first) | set(second)
        assert found == {w.id for w in live}, found
        assert done.id not in found and elsewhere.id not in found
        assert store.recovery_candidate_ids("other_queue", after_id=None, limit=10) == [elsewhere.id]
    finally:
        queue.close()
        store.close()


def test_recovery_settings_come_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.queue.processor.config import QueueProcessorConfig
    from stabilize.resilience.config import HandlerConfig

    monkeypatch.setenv("STABILIZE_RECOVERY_INTERVAL_SECONDS", "45")
    monkeypatch.setenv("STABILIZE_RECOVERY_MAX_AGE_HOURS", "72")
    monkeypatch.setenv("STABILIZE_RECOVERY_APPLICATION", "billing")
    monkeypatch.setenv("STABILIZE_RECOVER_ON_START", "true")
    config = QueueProcessorConfig.from_handler_config(HandlerConfig.from_env())
    assert config.recovery_interval_seconds == 45
    assert config.recovery_max_age_hours == 72
    assert config.recovery_application == "billing"
    assert config.recover_on_start is True
    assert config.enable_lock_heartbeat is True


def test_run_recovery_reports_and_can_raise(db: str) -> None:
    from stabilize.errors import RecoveryError
    from stabilize.queue.processor import QueueProcessor
    from stabilize.tasks.registry import TaskRegistry

    store, queue = _parts(db)
    processor = QueueProcessor(queue, store=store, task_registry=TaskRegistry())
    try:
        stale = _running_workflow(store, 48 * HOUR_MS)
        fresh = _running_workflow(store, HOUR_MS)
        processor.run_recovery()
        summary = processor.last_recovery
        assert summary.stale == 1 and summary.recovered == 1, summary
        assert {r.workflow_id for r in summary.results} == {stale.id, fresh.id}

        def unavailable(*args: Any, **kwargs: Any) -> list[str]:
            raise RuntimeError("database unavailable")

        store.recovery_candidate_ids = unavailable  # type: ignore[method-assign]
        with pytest.raises(RecoveryError):
            processor.run_recovery(strict=True)
        assert processor.run_recovery() == []
        assert processor.last_recovery.error is not None
    finally:
        queue.close()
        store.close()
