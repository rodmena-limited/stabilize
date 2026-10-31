"""WI-5 slice 5b: recovery sweeps coordinate, wake buffered configs once, and respect dead letters."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import RunTask, StartWaitingWorkflows
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")

HOUR_MS = 3_600_000


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgcoord") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'coord.db'}"
        return
    yield request.getfixturevalue("db")


def _parts(url: str, **queue_kwargs: Any) -> tuple[Any, Any]:
    if url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        return SqliteWorkflowStore(url, create_tables=True), SqliteQueue(url, **queue_kwargs)
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    return PostgresWorkflowStore(url), PostgresQueue(url, **queue_kwargs)


def _task() -> TaskExecution:
    return TaskExecution.create(name="t", implementing_class="noop", stage_start=True, stage_end=True)


def _stage(ref_id: str, **extra: Any) -> StageExecution:
    extra.setdefault("type", "noop")
    return StageExecution(ref_id=ref_id, name=ref_id.upper(), tasks=[_task()], **extra)


def _stranded(store: Any, queue_table: str = "queue_messages", config_id: str | None = None) -> Workflow:
    now = int(time.time() * 1000)
    wf = Workflow.create(application="coord", name="w", stages=[_stage("s0")])
    wf.status = WorkflowStatus.RUNNING
    wf.start_time = now - 60_000
    wf.created_at_ms = now - 60_000
    wf.queue_table = queue_table
    wf.pipeline_config_id = config_id
    stage = wf.stages[0]
    stage.status = WorkflowStatus.RUNNING
    stage.start_time = now - 60_000
    stage.tasks[0].status = WorkflowStatus.RUNNING
    stage.tasks[0].start_time = now - 60_000
    store.store(wf)
    return wf


def test_overlapping_sweeps_re_queue_a_lost_task_once(any_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import stabilize.recovery as recovery_module
    from stabilize.recovery import WorkflowRecovery

    store_a, queue_a = _parts(any_db)
    store_b, queue_b = _parts(any_db)
    try:
        wf = _stranded(store_a)
        planned = threading.Barrier(2)
        original_plan = recovery_module.plan_running_workflow

        def plan_then_meet(*args: Any, **kwargs: Any) -> Any:
            messages = original_plan(*args, **kwargs)
            try:
                planned.wait(timeout=2)
            except threading.BrokenBarrierError:
                pass
            return messages

        monkeypatch.setattr(recovery_module, "plan_running_workflow", plan_then_meet)
        outcomes: dict[str, list[Any]] = {}

        def sweep(name: str, store: Any, queue: Any) -> None:
            outcomes[name] = WorkflowRecovery(store, queue, max_recovery_age_hours=24).recover_pending_workflows()

        threads = [
            threading.Thread(target=sweep, args=("a", store_a, queue_a)),
            threading.Thread(target=sweep, args=("b", store_b, queue_b)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        statuses = sorted(r.status for name in ("a", "b") for r in outcomes[name] if r.workflow_id == wf.id)
        assert "recovered" in statuses, statuses
        assert statuses.count("recovered") == 1, statuses
        assert set(statuses) <= {"recovered", "locked", "skipped"}, statuses
        assert queue_a.size() == 1, "both sweeps pushed the lost task"
        message = queue_a.poll_one()
        assert isinstance(message, RunTask)
    finally:
        for h in (queue_a, queue_b, store_a, store_b):
            h.close()


def test_a_buffered_config_is_woken_once_and_only_when_a_slot_is_free(db: str) -> None:
    from stabilize.recovery import WorkflowRecovery

    store, queue = _parts(db)
    try:
        running = _stranded(store, config_id="cfg-1")
        running.max_concurrent_executions = 1
        running.is_limit_concurrent = True
        buffered = Workflow.create(application="coord", name="waiting", stages=[_stage("s0")])
        buffered.status = WorkflowStatus.BUFFERED
        buffered.pipeline_config_id = "cfg-1"
        buffered.max_concurrent_executions = 1
        buffered.is_limit_concurrent = True
        buffered.queue_table = "queue_messages"
        store.store(buffered)
        stage = store.retrieve_stage(running.stages[0].id)
        store.store_stage(stage)
        recovery = WorkflowRecovery(store, queue, max_recovery_age_hours=24)
        recovery.recover_pending_workflows()
        wakes = [m for m in _drain(queue) if isinstance(m, StartWaitingWorkflows)]
        assert wakes == [], "a buffered config was woken while its slot was taken"

        full = store.retrieve(running.id)
        full.status = WorkflowStatus.SUCCEEDED
        store.update_status(full)
        recovery.recover_pending_workflows()
        recovery.recover_pending_workflows()
        wakes = [m for m in _drain(queue) if isinstance(m, StartWaitingWorkflows)]
        assert len(wakes) == 1, f"the wake-up was pushed {len(wakes)} times"
        assert wakes[0].pipeline_config_id == "cfg-1"
    finally:
        queue.close()
        store.close()


def _drain(queue: Any) -> list[Any]:
    seen: list[Any] = []
    while True:
        m = queue.poll_one()
        if m is None:
            return seen
        seen.append(m)
        queue.ack(m)


def test_buffered_ids_are_scoped_to_the_queue_table_and_start_running_keeps_it(any_db: str) -> None:
    from stabilize.handlers import StartWorkflowHandler
    from stabilize.handlers.admission import start_running

    store, queue = _parts(any_db)
    try:
        here = Workflow.create(application="coord", name="here", stages=[_stage("s0")])
        here.status = WorkflowStatus.BUFFERED
        here.pipeline_config_id = "cfg-2"
        here.queue_table = "queue_messages"
        elsewhere = Workflow.create(application="coord", name="elsewhere", stages=[_stage("s0")])
        elsewhere.status = WorkflowStatus.BUFFERED
        elsewhere.pipeline_config_id = "cfg-2"
        elsewhere.queue_table = "other_queue"
        store.store(here)
        store.store(elsewhere)
        with store.transaction(queue) as txn:
            assert txn.buffered_ids("cfg-2", queue_table="queue_messages") == [here.id]
            assert txn.buffered_ids("cfg-2", queue_table="other_queue") == [elsewhere.id]
        handler = StartWorkflowHandler(queue, store)
        promoted = store.retrieve(elsewhere.id)
        with store.transaction(queue) as txn:
            start_running(handler, txn, promoted, promoted.initial_stages())
        assert store.retrieve(elsewhere.id).queue_table == "other_queue", "start_running re-stamped the queue table"
    finally:
        queue.close()
        store.close()


def test_a_dead_lettered_task_is_reported_and_re_pushed_only_after_new_activity(db: str) -> None:
    from stabilize.recovery import WorkflowRecovery

    store, queue = _parts(db, max_attempts=1, lock_duration=timedelta(minutes=5))
    try:
        wf = _stranded(store)
        task = wf.stages[0].tasks[0]
        queue.push(
            RunTask(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=wf.stages[0].id,
                task_id=task.id,
                task_type="noop",
            )
        )
        leased = queue.poll_one()
        assert leased is not None
        queue.reschedule(leased, timedelta(0))
        assert queue.check_and_move_expired() == 1
        assert queue.dlq_size() == 1

        recovery = WorkflowRecovery(store, queue, max_recovery_age_hours=24)
        results = recovery.recover_pending_workflows()
        mine = next(r for r in results if r.workflow_id == wf.id)
        assert mine.status == "dead_lettered", mine
        assert queue.size() == 0, "a dead-lettered task was re-pushed"

        time.sleep(1.1)
        stage = store.retrieve_stage(wf.stages[0].id)
        stage.context["poll"] = 1
        store.store_stage(stage)
        results = recovery.recover_pending_workflows()
        mine = next(r for r in results if r.workflow_id == wf.id)
        assert mine.status == "dead_letter_stale", mine
        assert queue.size() == 1
    finally:
        queue.clear_dlq()
        queue.close()
        store.close()
