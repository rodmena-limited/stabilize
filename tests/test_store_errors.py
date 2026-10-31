"""WI-7 slice 7a: store outages are redelivered, SQLSTATE transients, the cause-chain walk, retry_after."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize.errors import TransientError
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import CompleteStage, RunTask, StartStage
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgerr") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'errors.db'}"
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


def _transaction_class(store: Any) -> type:
    if type(store).__name__.startswith("Sqlite"):
        from stabilize.persistence.sqlite.transaction import AtomicTransaction

        return AtomicTransaction
    from stabilize.persistence.postgres.transaction import PostgresTransaction

    return PostgresTransaction


def _outage(url: str) -> Exception:
    if url.startswith("sqlite"):
        return sqlite3.OperationalError("disk I/O error")
    return psycopg.OperationalError("server closed the connection unexpectedly")


class _Noop(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class _Busy(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        raise TransientError("upstream is busy", retry_after=7)


def _processor(store: Any, queue: Any) -> Any:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.registry import TaskRegistry

    registry = TaskRegistry()
    registry.register("noop", _Noop)
    registry.register("busy", _Busy)
    return QueueProcessor(
        queue,
        store=store,
        task_registry=registry,
        config=QueueProcessorConfig(enable_deduplication=False, retry_delay=timedelta(milliseconds=100)),
        handler_config=HandlerConfig(
            handler_retry_delay_seconds=0.1, task_backoff_min_delay_ms=5, task_backoff_max_delay_ms=10
        ),
    )


def _one_stage_workflow(
    store: Any,
    task_type: str,
    *,
    stage_status: WorkflowStatus,
    task_status: WorkflowStatus | None = None,
) -> tuple[Workflow, StageExecution]:
    task = TaskExecution.create(name="t", implementing_class=task_type, stage_start=True, stage_end=True)
    stage = StageExecution(ref_id="s0", type=task_type, name="S0", tasks=[task])
    wf = Workflow.create(application="outage", name="w", stages=[stage])
    now = int(time.time() * 1000)
    wf.status = WorkflowStatus.RUNNING
    wf.start_time = now
    wf.queue_table = "queue_messages"
    if stage_status is not WorkflowStatus.NOT_STARTED:
        stage.status = stage_status
        stage.start_time = now
        task.status = task_status or WorkflowStatus.SUCCEEDED
        task.start_time = now
        task.end_time = now if task.status is WorkflowStatus.SUCCEEDED else None
    store.store(wf)
    return wf, stage


def _fail_first_save(monkeypatch: pytest.MonkeyPatch, store: Any, stage_id: str, outage: Exception) -> list[str]:
    txn_class = _transaction_class(store)
    original = txn_class.store_stage
    failures: list[str] = []

    def fail_once(self: Any, s: Any, expected_phase: str | None = None) -> None:
        if s.id == stage_id and not failures:
            failures.append(s.id)
            raise outage
        return original(self, s, expected_phase)

    monkeypatch.setattr(txn_class, "store_stage", fail_once)
    return failures


def _drive(processor: Any, until: Any, budget: float = 10.0) -> None:
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        try:
            processor.process_one()
        except Exception:
            pass
        if until():
            return
        time.sleep(0.05)


def test_a_store_outage_while_starting_a_stage_is_redelivered_not_terminal(
    any_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.models.stage.stage import PLANNING_FAILED

    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        wf, stage = _one_stage_workflow(store, "noop", stage_status=WorkflowStatus.NOT_STARTED)
        failures = _fail_first_save(monkeypatch, store, stage.id, _outage(any_db))
        queue.push(StartStage(execution_type="PIPELINE", execution_id=wf.id, stage_id=stage.id))
        raised: Exception | None = None
        try:
            processor.process_one()
        except Exception as e:
            raised = e
        assert failures == [stage.id], "the outage never fired; the pin is vacuous"
        after = store.retrieve_stage(stage.id)
        assert after.status is not WorkflowStatus.TERMINAL, "a store outage ended the stage"
        assert "exception" not in after.context and PLANNING_FAILED not in after.context, (
            f"the outage was recorded as a planning failure: {after.context}"
        )
        assert raised is not None, "the handler swallowed the outage instead of letting the message be redelivered"
        assert queue.size() == 1, "the StartStage message is not waiting for redelivery"
        _drive(processor, lambda: store.retrieve_stage(stage.id).status is not WorkflowStatus.NOT_STARTED)
        assert store.retrieve_stage(stage.id).status in (WorkflowStatus.RUNNING, WorkflowStatus.SUCCEEDED)
    finally:
        queue.close()
        store.close()


def test_a_store_outage_while_completing_a_stage_is_redelivered_not_terminal(
    any_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        wf, stage = _one_stage_workflow(store, "noop", stage_status=WorkflowStatus.RUNNING)
        failures = _fail_first_save(monkeypatch, store, stage.id, _outage(any_db))
        queue.push(CompleteStage(execution_type="PIPELINE", execution_id=wf.id, stage_id=stage.id))
        raised: Exception | None = None
        try:
            processor.process_one()
        except Exception as e:
            raised = e
        assert failures == [stage.id], "the outage never fired; the pin is vacuous"
        after = store.retrieve_stage(stage.id)
        assert after.status is WorkflowStatus.RUNNING, f"a store outage moved the stage to {after.status.name}"
        assert "exception" not in after.context
        assert raised is not None
        _drive(processor, lambda: store.retrieve_stage(stage.id).status.is_complete)
        assert store.retrieve_stage(stage.id).status is WorkflowStatus.SUCCEEDED
    finally:
        queue.close()
        store.close()


def test_is_transient_knows_postgres_connection_and_contention_codes(monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.errors import is_permanent, is_transient

    errors = psycopg.errors
    retried = (
        errors.SerializationFailure,
        errors.DeadlockDetected,
        errors.LockNotAvailable,
        errors.TooManyConnections,
        errors.CannotConnectNow,
        errors.SqlclientUnableToEstablishSqlconnection,
        errors.SqlserverRejectedEstablishmentOfSqlconnection,
        errors.ConnectionFailure,
        errors.ConnectionDoesNotExist,
    )
    for cls in retried:
        assert is_transient(cls("x")), f"{cls.__name__} ({cls.sqlstate}) is not retried"
        assert not is_permanent(cls("x")), cls.__name__
    monkeypatch.delenv("STABILIZE_RETRY_ON_SHUTDOWN", raising=False)
    for cls in (errors.AdminShutdown, errors.CrashShutdown, errors.QueryCanceled, errors.UndefinedTable):
        assert not is_transient(cls("x")), f"{cls.__name__} ({cls.sqlstate}) is retried by default"
    monkeypatch.setenv("STABILIZE_RETRY_ON_SHUTDOWN", "1")
    for cls in (errors.AdminShutdown, errors.CrashShutdown, errors.QueryCanceled):
        assert is_transient(cls("x")), f"{cls.__name__} is not retried under STABILIZE_RETRY_ON_SHUTDOWN"
    assert not is_transient(errors.UndefinedTable("x"))
    assert is_permanent(errors.UndefinedTable("x")) is False, "an unknown SQLSTATE is neither transient nor permanent"


def test_the_cause_chain_walk_covers_cause_and_context_and_stops_at_a_cycle() -> None:
    from stabilize.errors import StabilizeError, is_permanent, is_transient

    assert is_transient(StabilizeError("wrapped", cause=TransientError("inner"))), "StabilizeError.cause is not walked"

    try:
        try:
            raise TransientError("first")
        except TransientError:
            raise RuntimeError("raised while handling")
    except RuntimeError as e:
        implicit = e
    assert is_transient(implicit), "__context__ is not walked when there is no __cause__"

    try:
        try:
            raise TransientError("first")
        except TransientError:
            raise RuntimeError("detached") from None
    except RuntimeError as e:
        detached = e
    assert not is_transient(detached), "a suppressed context was walked"

    try:
        try:
            raise TransientError("first")
        except TransientError:
            raise ValueError("bad input")
    except ValueError as e:
        permanent_outer = e
    assert not is_transient(permanent_outer) and is_permanent(permanent_outer)

    a = RuntimeError("a")
    b = RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a
    assert is_transient(a) is False
    assert is_permanent(a) is False


@pytest.mark.timeout(10)
def test_error_chain_terminates_on_a_cycle() -> None:
    from stabilize.error_codes import error_chain

    a = RuntimeError("a")
    b = RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a
    chain = error_chain(a)
    assert len(chain) == 2 and chain[-1] is a


def test_retry_after_extends_the_task_backoff(any_db: str) -> None:
    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        wf, stage = _one_stage_workflow(
            store, "busy", stage_status=WorkflowStatus.RUNNING, task_status=WorkflowStatus.RUNNING
        )
        task = stage.tasks[0]
        queue.push(
            RunTask(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=stage.id,
                task_id=task.id,
                task_type="busy",
            )
        )
        assert processor.process_one() is True
        wait = queue.next_ready_in()
        assert wait is not None, "no retry was scheduled"
        assert wait >= 6.0, f"the retry is due in {wait:.2f}s; retry_after=7 was ignored"
    finally:
        queue.close()
        store.close()
