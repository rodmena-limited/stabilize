"""WI-7 slice 7b: a store outage spends a redelivery budget, not the attempt count."""

from __future__ import annotations

import sqlite3
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
from stabilize.queue.messages import StartStage
from stabilize.queue.processor.config import QueueProcessorConfig
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgbudget") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'budget.db'}"
        return
    yield request.getfixturevalue("db")


def _queue(url: str, **kwargs: Any) -> Any:
    if url.startswith("sqlite"):
        from stabilize.queue.sqlite import SqliteQueue

        queue = SqliteQueue(url, **kwargs)
        queue._create_table()
        return queue
    from stabilize.queue.postgres import PostgresQueue

    return PostgresQueue(url, **kwargs)


def _store(url: str) -> Any:
    if url.startswith("sqlite"):
        from stabilize.persistence.sqlite import SqliteWorkflowStore

        return SqliteWorkflowStore(url, create_tables=True)
    from stabilize.persistence.postgres import PostgresWorkflowStore

    return PostgresWorkflowStore(url)


def _message() -> StartStage:
    return StartStage(
        execution_type="PIPELINE", execution_id="01M68P00000000000000000000", stage_id="01M68P00000000000000000001"
    )


def test_a_store_outage_keeps_the_message_deliverable_past_max_attempts(any_db: str) -> None:
    queue = _queue(any_db, max_attempts=2, store_retry_budget=timedelta(minutes=30))
    try:
        queue.push(_message())
        for _ in range(3):
            delivered = queue.poll_one()
            assert delivered is not None, "the message stopped being delivered while the store budget was open"
            queue.reschedule(delivered, timedelta(0), store_unavailable=True)
        again = queue.poll_one()
        assert again is not None, "attempts reached max_attempts and the store budget did not keep the message alive"
        assert again.attempts == 4
        assert again.store_retries == 3
        assert queue.check_and_move_expired() == 0, "a message inside its store budget was dead-lettered"
        queue.reschedule(again, timedelta(0), store_unavailable=True)
        assert queue.next_ready_in() is not None, "the next-ready estimate ignores the store budget"
    finally:
        queue.clear()
        queue.clear_dlq()
        queue.close()


def test_an_exhausted_store_budget_moves_the_message_to_the_dlq_with_its_cause(any_db: str) -> None:
    queue = _queue(any_db, max_attempts=1, store_retry_budget=timedelta(seconds=1))
    try:
        queue.push(_message())
        first = queue.poll_one()
        assert first is not None
        queue.reschedule(first, timedelta(0), store_unavailable=True)
        second = queue.poll_one()
        assert second is not None, "the open budget did not carry the message past max_attempts"
        queue.reschedule(second, timedelta(0), store_unavailable=True)
        assert queue.check_and_move_expired() == 0, "the budget was treated as exhausted at once"
        time.sleep(1.3)
        assert queue.poll_one() is None, "a message past its store budget and its attempts was still delivered"
        assert queue.check_and_move_expired() == 1
        [dead] = queue.list_dlq()
        assert dead["error"].startswith("store_unavailable_budget_exhausted"), dead["error"]
        assert dead["attempts"] == 2
        assert dead["store_retries"] == 2
    finally:
        queue.clear()
        queue.clear_dlq()
        queue.close()


def test_a_failure_that_is_not_the_store_closes_the_budget(any_db: str) -> None:
    queue = _queue(any_db, max_attempts=2, store_retry_budget=timedelta(minutes=30))
    try:
        queue.push(_message())
        first = queue.poll_one()
        assert first is not None
        queue.reschedule(first, timedelta(0), store_unavailable=True)
        second = queue.poll_one()
        assert second is not None and second.store_retries == 1
        queue.reschedule(second, timedelta(0))
        assert queue.poll_one() is None, "a message at max_attempts with a closed budget was delivered"
        assert queue.check_and_move_expired() == 1
        [dead] = queue.list_dlq()
        assert dead["error"].startswith("Exceeded max_attempts"), dead["error"]
        assert dead["store_retries"] == 0
    finally:
        queue.clear()
        queue.clear_dlq()
        queue.close()


def test_the_processor_backs_off_by_store_retries(any_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.tasks.registry import TaskRegistry

    store = _store(any_db)
    queue = _queue(any_db)
    try:
        task = TaskExecution.create(name="t", implementing_class="noop", stage_start=True, stage_end=True)
        stage = StageExecution(ref_id="s0", type="noop", name="S0", tasks=[task])
        wf = Workflow.create(application="budget", name="w", stages=[stage])
        wf.status = WorkflowStatus.RUNNING
        wf.start_time = int(time.time() * 1000)
        wf.queue_table = "queue_messages"
        store.store(wf)
        txn_class: type
        if any_db.startswith("sqlite"):
            from stabilize.persistence.sqlite.transaction import AtomicTransaction

            txn_class = AtomicTransaction
            outage: Exception = sqlite3.OperationalError("disk I/O error")
        else:
            from stabilize.persistence.postgres.transaction import PostgresTransaction

            txn_class = PostgresTransaction
            outage = psycopg.OperationalError("server closed the connection unexpectedly")

        def always_fail(self: Any, s: Any, expected_phase: str | None = None) -> None:
            raise outage

        monkeypatch.setattr(txn_class, "store_stage", always_fail)
        processor = QueueProcessor(
            queue,
            store=store,
            task_registry=TaskRegistry(),
            config=QueueProcessorConfig(enable_deduplication=False, retry_delay=timedelta(milliseconds=500)),
        )
        queue.push(StartStage(execution_type="PIPELINE", execution_id=wf.id, stage_id=stage.id))
        with pytest.raises(type(outage)):
            processor.process_one()
        first_wait = queue.next_ready_in()
        assert first_wait is not None and first_wait <= 0.55, first_wait
        time.sleep(0.6)
        with pytest.raises(type(outage)):
            processor.process_one()
        second_wait = queue.next_ready_in()
        assert second_wait is not None and second_wait >= 0.8, (
            f"the second redelivery is due in {second_wait:.2f}s; the delay did not grow with store_retries"
        )
    finally:
        queue.clear()
        queue.close()
        store.close()


def test_queue_tables_old_and_new_carry_the_budget_columns(any_db: str) -> None:
    if any_db.startswith("sqlite"):
        from stabilize.queue.sqlite import SqliteQueue

        path = any_db.removeprefix("sqlite:///")
        created = SqliteQueue(any_db, table_name="custom")
        created._create_table()
        created.close()
        with sqlite3.connect(path) as conn:
            for table in ("custom", "custom_dlq"):
                conn.execute(f"ALTER TABLE {table} DROP COLUMN store_retries")
                conn.execute(f"ALTER TABLE {table} DROP COLUMN store_retry_since")
            conn.commit()
        reopened = SqliteQueue(any_db, table_name="custom")
        try:
            with sqlite3.connect(path) as conn:
                for table in ("custom", "custom_dlq"):
                    present = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
                    assert {"store_retries", "store_retry_since"} <= present, (table, present)
            reopened.push(_message())
            assert reopened.poll_one() is not None
        finally:
            reopened.clear()
            reopened.close()
        return
    assert stabilize("mg-queue-table", "custom", "--db-url", any_db).returncode == 0
    with psycopg.connect(any_db) as conn:
        for table in ("queue_messages", "queue_messages_dlq", "custom", "custom_dlq"):
            rows = conn.execute(
                "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns "
                "WHERE table_name = %s AND column_name IN ('store_retries', 'store_retry_since') ORDER BY column_name",
                (table,),
            ).fetchall()
            assert [r[0] for r in rows] == ["store_retries", "store_retry_since"], (table, rows)
            assert rows[0][1:] == ("integer", "NO", "0"), (table, rows[0])
            assert rows[1][1] == "timestamp with time zone", (table, rows[1])
