"""WI-5 slice 5c: a signal is consumed, buffered and discarded inside the transaction that owns the stage change."""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.errors import ConcurrencyError
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import RunTask, SignalStage
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgsig") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'signals.db'}"
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


class _Suspend(Task):
    runs: list[str] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        _Suspend.runs.append(stage.ref_id)
        if stage.context.get("_signal_name"):
            return TaskResult.success(outputs={"signal": stage.context["_signal_name"]})
        return TaskResult(status=WorkflowStatus.SUSPENDED)


def _processor(store: Any, queue: Any, **config: Any) -> Any:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.registry import TaskRegistry

    registry = TaskRegistry()
    registry.register("suspend_sig", _Suspend)
    return QueueProcessor(
        queue,
        store=store,
        task_registry=registry,
        config=QueueProcessorConfig(**config),
        handler_config=HandlerConfig(handler_retry_delay_seconds=0.1),
    )


def _running_wait(store: Any) -> tuple[Workflow, StageExecution]:
    task = TaskExecution.create(name="t", implementing_class="suspend_sig", stage_start=True, stage_end=True)
    stage = StageExecution(ref_id="s0", type="suspend_sig", name="S0", tasks=[task])
    wf = Workflow.create(application="signals", name="w", stages=[stage])
    now = int(time.time() * 1000)
    wf.status = WorkflowStatus.RUNNING
    wf.start_time = now
    wf.queue_table = "queue_messages"
    stage.status = WorkflowStatus.RUNNING
    stage.start_time = now
    task.status = WorkflowStatus.RUNNING
    task.start_time = now
    store.store(wf)
    return wf, stage


def _transaction_class(store: Any) -> type:
    if type(store).__name__.startswith("Sqlite"):
        from stabilize.persistence.sqlite.transaction import AtomicTransaction

        return AtomicTransaction
    from stabilize.persistence.postgres.transaction import PostgresTransaction

    return PostgresTransaction


def test_a_consumed_signal_survives_a_save_that_rolls_back(any_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.persistence.transaction import DEADLOCK_RETRY_POLICY

    _Suspend.runs.clear()
    store, queue = _parts(any_db)
    processor = _processor(store, queue, enable_deduplication=False)
    try:
        wf, stage = _running_wait(store)
        assert store.buffer_signal(wf.id, "s0", "go", {"k": 1}) > 0
        txn_class = _transaction_class(store)
        original = txn_class.store_stage
        budget = int(DEADLOCK_RETRY_POLICY.max_attempts)
        failures: list[str] = []

        def fail_every_save_of_the_resumed_stage(self: Any, s: Any, expected_phase: str | None = None) -> None:
            if (
                s.id == stage.id
                and s.status == WorkflowStatus.RUNNING
                and s.context.get("_signal_name")
                and len(failures) < budget
            ):
                failures.append(s.id)
                raise ConcurrencyError("version moved under the resume")
            return original(self, s, expected_phase)

        monkeypatch.setattr(txn_class, "store_stage", fail_every_save_of_the_resumed_stage)
        queue.push(
            RunTask(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=stage.id,
                task_id=stage.tasks[0].id,
                task_type="suspend_sig",
            )
        )
        assert processor.process_one() is True
        assert len(failures) == budget, "the injected conflicts never exhausted the save's retries; the pin is vacuous"
        resumed = store.retrieve_stage(stage.id)
        assert resumed.status == WorkflowStatus.RUNNING, (
            f"the stage is {resumed.status.name}: the signal was consumed by a transaction that rolled back"
        )
        assert resumed.context.get("_signal_name") == "go"
        assert store.pending_signal_count(wf.id, "s0") == 0
        follow_up = queue.poll_one()
        assert isinstance(follow_up, RunTask)
    finally:
        queue.close()
        store.close()


def test_a_persistent_signal_is_buffered_once_across_a_redelivery(any_db: str) -> None:
    from stabilize.errors import DuplicateDeliveryError

    store, queue = _parts(any_db)
    processor = _processor(store, queue, enable_deduplication=False)
    try:
        wf, stage = _running_wait(store)
        queue.push(
            SignalStage(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=stage.id,
                signal_name="later",
                signal_data={"n": 1},
                persistent=True,
            )
        )
        message = queue.poll_one()
        assert isinstance(message, SignalStage)
        processor._handle_message(message)
        assert store.pending_signal_count(wf.id, "s0") == 1
        with pytest.raises(DuplicateDeliveryError):
            processor._handle_message(message)
        assert store.pending_signal_count(wf.id, "s0") == 1, "the redelivery buffered the signal a second time"
    finally:
        queue.close()
        store.close()


def test_recovery_re_pushes_a_signal_for_an_unconsumed_row(any_db: str) -> None:
    from stabilize.recovery import WorkflowRecovery

    store, queue = _parts(any_db)
    try:
        wf, stage = _running_wait(store)
        suspended = store.retrieve_stage(stage.id)
        suspended.status = WorkflowStatus.SUSPENDED
        suspended.tasks[0].status = WorkflowStatus.SUSPENDED
        store.store_stage(suspended)
        row_id = store.buffer_signal(wf.id, "s0", "go", {"k": 2})
        assert row_id > 0
        results = WorkflowRecovery(store, queue, max_recovery_age_hours=24).recover_pending_workflows()
        mine = next(r for r in results if r.workflow_id == wf.id)
        assert mine.status == "recovered", mine
        message = queue.poll_one()
        assert isinstance(message, SignalStage), message
        assert message.signal_name == "go" and message.signal_row_id == row_id
        assert queue.poll_one() is None
    finally:
        queue.close()
        store.close()


def test_signal_storage_degrades_only_on_a_missing_table_or_grant(db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    store = PostgresWorkflowStore(db)
    try:
        wf, _ = _running_wait(store)
        assert store.supports_signal_storage() is True

        class _Down:
            def __enter__(self) -> Any:
                raise psycopg.OperationalError("server closed the connection unexpectedly")

            def __exit__(self, *exc: Any) -> None:
                return None

        monkeypatch.setattr(store, "_borrow", lambda: _Down())
        with pytest.raises(psycopg.OperationalError):
            store.consume_signal(wf.id, "s0")
        assert store.supports_signal_storage() is True, "a failover disabled signal storage"
        monkeypatch.undo()

        class _Gone:
            def __enter__(self) -> Any:
                raise psycopg.errors.UndefinedTable('relation "workflow_signals" does not exist')

            def __exit__(self, *exc: Any) -> None:
                return None

        monkeypatch.setattr(store, "_borrow", lambda: _Gone())
        assert store.consume_signal(wf.id, "s0") is None
        assert store.supports_signal_storage() is False
    finally:
        monkeypatch.undo()
        store.close()
