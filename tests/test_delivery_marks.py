"""Rule 7: a delivered message is marked processed in the transaction that pushes its successors (WI-3)."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import CompleteWorkflow, StartStage, StartWorkflow
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.queue.processor.handler_base import MessageHandler
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


class _NoOp(Task):
    def execute(self, stage: Any) -> TaskResult:
        return TaskResult.success()


class _Slow(Task):
    seconds = 2.5

    def execute(self, stage: Any) -> TaskResult:
        time.sleep(self.seconds)
        return TaskResult.success()


class _Brief(Task):
    def execute(self, stage: Any) -> TaskResult:
        time.sleep(0.4)
        return TaskResult.success()


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgmark") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


def _task(task_type: str) -> TaskExecution:
    return TaskExecution.create(name="t", implementing_class=task_type, stage_start=True, stage_end=True)


def _stage(ref_id: str, task_type: str, **extra: Any) -> StageExecution:
    return StageExecution(ref_id=ref_id, type=task_type, name=ref_id.upper(), tasks=[_task(task_type)], **extra)


def _parts(url: str) -> tuple[Any, Any]:
    if url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        return SqliteWorkflowStore(url, create_tables=True), SqliteQueue(url)
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    return PostgresWorkflowStore(url), PostgresQueue(url)


def _processor(store: Any, queue: Any, handler_retry_seconds: float = 15.0, **config: Any) -> Any:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.registry import TaskRegistry

    registry = TaskRegistry()
    registry.register("noop_marks", _NoOp)
    registry.register("slow_marks", _Slow)
    registry.register("brief_marks", _Brief)
    return QueueProcessor(
        queue,
        store=store,
        task_registry=registry,
        config=QueueProcessorConfig(**config),
        handler_config=HandlerConfig(handler_retry_delay_seconds=handler_retry_seconds),
    )


class _Leaky(MessageHandler[StartWorkflow]):
    def __init__(self, queue: Any) -> None:
        self.queue = queue

    @property
    def message_type(self) -> type[StartWorkflow]:
        return StartWorkflow

    def handle(self, message: StartWorkflow) -> None:
        self.queue.push(CompleteWorkflow(execution_type="PIPELINE", execution_id=message.execution_id))


class _Swallow(MessageHandler[CompleteWorkflow]):
    @property
    def message_type(self) -> type[CompleteWorkflow]:
        return CompleteWorkflow

    def handle(self, message: CompleteWorkflow) -> None:
        return None


def test_the_guard_flags_a_handler_that_pushes_outside_its_transaction(
    db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.errors import DeliveryContractError
    from stabilize.queue.postgres import PostgresQueue
    from stabilize.queue.processor import QueueProcessor

    queue = PostgresQueue(db)
    processor = QueueProcessor(queue, config=QueueProcessorConfig(enable_deduplication=False))
    processor.register_handler(_Leaky(queue))
    processor.register_handler(_Swallow())
    try:
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01LEAK000000000000000001"))
        assert processor.process_one() is True
        assert len(processor.delivery_defects) == 1
        defect = processor.delivery_defects[0]
        assert defect.message_type == "StartWorkflow"
        assert any(
            "CompleteWorkflow" in reason and "outside any store transaction" in reason for reason in defect.reasons
        )

        monkeypatch.setenv("STABILIZE_STRICT_DELIVERY", "1")
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01LEAK000000000000000002"))
        with pytest.raises(DeliveryContractError, match="StartWorkflow"):
            processor.process_all(timeout=30)
        assert queue.size() == 0, "strict mode must not leave the delivered message unacknowledged"
    finally:
        queue.close()


def test_the_happy_path_and_the_waiting_paths_mark_inside_their_pushing_transactions(
    db: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from stabilize.models.status import WorkflowStatus
    from stabilize.orchestrator import Orchestrator

    monkeypatch.setenv("STABILIZE_STRICT_DELIVERY", "1")
    store, queue = _parts(db)
    processor = _processor(store, queue, handler_retry_seconds=0.2, retry_delay=timedelta(milliseconds=200))
    try:
        chain = Workflow.create(
            application="marks",
            name="chain",
            stages=[_stage("s0", "noop_marks"), _stage("s1", "noop_marks", requisite_stage_ref_ids={"s0"})],
        )
        mutex = Workflow.create(
            application="marks",
            name="mutex",
            stages=[
                _stage("m0", "brief_marks", mutex_key="one-at-a-time"),
                _stage("m1", "brief_marks", mutex_key="one-at-a-time"),
            ],
        )
        choice = Workflow.create(
            application="marks",
            name="choice",
            stages=[
                _stage("c0", "brief_marks", deferred_choice_group="g"),
                _stage("c1", "brief_marks", deferred_choice_group="g"),
            ],
        )
        orchestrator = Orchestrator(queue, store=store)
        for wf in (chain, mutex, choice):
            orchestrator.start(wf)
        with caplog.at_level(logging.DEBUG, logger="stabilize.handlers.start_stage.handler"):
            processor.process_all(timeout=90)
        assert store.retrieve(chain.id).status == WorkflowStatus.SUCCEEDED
        assert store.retrieve(mutex.id).status == WorkflowStatus.SUCCEEDED
        settled = store.retrieve(choice.id)
        assert {s.status for s in settled.stages} == {WorkflowStatus.SUCCEEDED, WorkflowStatus.CANCELED}
        assert any("mutex" in r.getMessage() for r in caplog.records), "the mutex wait path never ran"
        assert processor.delivery_defects == [], processor.delivery_defects
    finally:
        queue.close()
        store.close()


def test_complete_workflow_requeues_itself_inside_a_marking_transaction(db: str) -> None:
    store, queue = _parts(db)
    processor = _processor(store, queue, handler_retry_seconds=0.0, retry_delay=timedelta(0))
    try:
        wf = Workflow.create(application="marks", name="idle", stages=[_stage("s0", "noop_marks")])
        store.store(wf)
        queue.push(CompleteWorkflow(execution_type="PIPELINE", execution_id=wf.id))
        assert processor.process_one() is True
        assert processor.delivery_defects == [], processor.delivery_defects
        requeued = queue.poll_one()
        assert isinstance(requeued, CompleteWorkflow) and requeued.retry_count == 1, requeued
    finally:
        queue.close()
        store.close()


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'marks.db'}"
        return
    yield request.getfixturevalue("db")


def test_a_redelivered_message_is_acked_without_pushing_again(any_db: str) -> None:
    from stabilize.errors import DuplicateDeliveryError

    store, queue = _parts(any_db)
    processor = _processor(store, queue, enable_deduplication=False)
    try:
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01NOSUCHWORKFLOW00000001"))
        first = queue.poll_one()
        assert isinstance(first, StartWorkflow) and first.message_id
        processor._handle_message(first)
        assert queue.size() == 2, "the first delivery pushes InvalidWorkflowId beside the leased message"
        with pytest.raises(DuplicateDeliveryError):
            processor._handle_message(first)
        assert queue.size() == 2, "the redelivery pushed again"
        queue.reschedule(first, timedelta(0))
        assert processor.process_one() is True
        assert processor.process_one() is True
        assert queue.size() == 0, "process_one did not acknowledge the duplicate delivery"
        assert queue.poll_one() is None
    finally:
        queue.close()
        store.close()


def test_process_one_keeps_the_lease_alive_while_its_handler_runs(db: str) -> None:
    from stabilize.orchestrator import Orchestrator
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    store = PostgresWorkflowStore(db)
    queue = PostgresQueue(db, lock_duration=timedelta(seconds=1))
    other = PostgresQueue(db, lock_duration=timedelta(seconds=1))
    processor = _processor(store, queue)
    seen_by_other: list[Any] = []
    try:
        wf = Workflow.create(application="lease", name="w", stages=[_stage("s0", "slow_marks")])
        Orchestrator(queue, store=store).start(wf)
        while True:
            next_message = queue.poll_one()
            assert next_message is not None, "the workflow never reached its RunTask"
            if type(next_message).__name__ == "RunTask":
                queue.reschedule(next_message, timedelta(0))
                break
            processor._handle_message(next_message)
            queue.ack(next_message)

        def poll_during_run() -> None:
            time.sleep(1.6)
            seen_by_other.append(other.poll_one())

        probe = threading.Thread(target=poll_during_run)
        probe.start()
        processor.process_one()
        probe.join(timeout=10)
        assert seen_by_other == [None], "the lease lapsed during process_one and another consumer took the message"
    finally:
        other.close()
        queue.close()
        store.close()


def test_the_dlq_sweep_leaves_a_leased_message_alone(any_db: str) -> None:
    store, queue = _parts(any_db)
    store.close()
    queue.close()
    if any_db.startswith("sqlite"):
        from stabilize.queue.sqlite import SqliteQueue

        queue = SqliteQueue(any_db, max_attempts=1, lock_duration=timedelta(minutes=5))
    else:
        from stabilize.queue.postgres import PostgresQueue

        queue = PostgresQueue(any_db, max_attempts=1, lock_duration=timedelta(minutes=5))
    try:
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01DLQ0000000000000000001"))
        leased = queue.poll_one()
        assert leased is not None and leased.attempts == 1
        assert queue.check_and_move_expired() == 0, "a message still under lease was moved to the DLQ"
        assert queue.dlq_size() == 0
        queue.reschedule(leased, timedelta(0))
        assert queue.check_and_move_expired() == 1
        assert queue.dlq_size() == 1
    finally:
        queue.clear_dlq()
        queue.close()


def test_a_transactional_push_on_sqlite_is_swept_by_the_queue_rule_not_the_message_default(tmp_path: Path) -> None:
    from stabilize.persistence.sqlite import SqliteWorkflowStore
    from stabilize.queue.sqlite import SqliteQueue

    url = f"sqlite:///{tmp_path / 'sweep.db'}"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url, max_attempts=1, lock_duration=timedelta(minutes=5))
    try:
        with store.transaction(queue) as txn:
            txn.push_message(
                StartStage(execution_type="PIPELINE", execution_id="01SWEEP000000000000000001", stage_id="s")
            )
        leased = queue.poll_one()
        assert leased is not None
        queue.reschedule(leased, timedelta(0))
        assert queue.poll_one() is None, "the poll rule says this message is exhausted"
        assert queue.check_and_move_expired() == 1, "the sweep rule disagreed with the poll rule and left it stuck"
    finally:
        queue.close()
        store.close()


def test_deadlocks_are_recognised_by_sqlstate_not_by_english_text() -> None:
    from stabilize.persistence.transaction import is_deadlock_error

    german = psycopg.errors.lookup("40P01")("Verklemmung entdeckt")
    assert is_deadlock_error(german) is True
    serialization = psycopg.errors.lookup("40001")(
        "konnte Zugriff wegen gleichzeitiger Aktualisierung nicht serialisieren"
    )
    assert is_deadlock_error(serialization) is True
    other = psycopg.errors.lookup("23505")("duplicate key value violates unique constraint")
    assert is_deadlock_error(other) is False
