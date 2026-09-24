"""A message is marked processed once when its handler's transaction already marked it (#46)."""

from __future__ import annotations

from typing import Any

import pytest

from stabilize import Orchestrator, QueueProcessor, Task, TaskRegistry, TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.committed_marks import clear_committed, consume_committed, record_committed
from stabilize.queue.messages import StartWorkflow

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


class _NoOp(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def test_consume_is_exact_and_one_shot() -> None:
    clear_committed()
    record_committed(["m1"])
    assert consume_committed("m2") is False
    assert consume_committed("m1") is True
    assert consume_committed("m1") is False


class _CountingStore:
    def __init__(self) -> None:
        self.marked: list[str] = []

    def is_message_processed(self, message_id: str) -> bool:
        return False

    def mark_message_processed(self, message_id: str, handler_type: Any = None, execution_id: Any = None) -> None:
        self.marked.append(message_id)


class _Handler:
    message_type = StartWorkflow

    def __init__(self, commits_mark: bool) -> None:
        self.commits_mark = commits_mark

    def handle(self, message: Any) -> None:
        if self.commits_mark:
            record_committed([message.message_id])


def _processor_with(handler: _Handler, store: _CountingStore) -> QueueProcessor:
    from stabilize.queue import SqliteQueue

    processor = QueueProcessor(SqliteQueue("sqlite:///:memory:"), store=None)
    processor._store = store  # type: ignore[assignment]
    processor._handlers[StartWorkflow] = handler  # type: ignore[assignment]
    return processor


def _message(message_id: str) -> StartWorkflow:
    message = StartWorkflow(execution_type="workflow", execution_id="e1")
    message.message_id = message_id
    return message


def test_handler_that_committed_its_mark_is_not_marked_again() -> None:
    store = _CountingStore()
    _processor_with(_Handler(commits_mark=True), store)._handle_message(_message("m-committed"))
    assert store.marked == []


def test_handler_that_did_not_mark_is_still_marked_by_the_processor() -> None:
    store = _CountingStore()
    _processor_with(_Handler(commits_mark=False), store)._handle_message(_message("m-unmarked"))
    assert store.marked == ["m-unmarked"]


def test_a_stale_committed_id_does_not_suppress_a_different_message() -> None:
    store = _CountingStore()
    record_committed(["m-stale"])
    _processor_with(_Handler(commits_mark=False), store)._handle_message(_message("m-new"))
    assert store.marked == ["m-new"]


def _count_processed_inserts(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    import psycopg

    seen: list[str] = []
    real = psycopg.Cursor.execute

    def execute(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        text = query.as_string(None) if hasattr(query, "as_string") else str(query)
        if "INSERT INTO processed_messages" in text:
            seen.append(text)
        return real(self, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", execute)
    return seen


def test_one_stage_workflow_marks_each_message_once_in_postgres(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue import PostgresQueue

    store = PostgresWorkflowStore(postgres_url)
    queue = PostgresQueue(postgres_url, table_name="queue_messages")
    try:
        queue.clear()
        registry = TaskRegistry()
        registry.register("noop_mark_once", _NoOp)
        processor = QueueProcessor(queue, store=store, task_registry=registry)
        workflow = Workflow.create(
            application="mark-once",
            name="one-stage",
            stages=[
                StageExecution(
                    ref_id="s1",
                    type="noop_mark_once",
                    name="S1",
                    tasks=[
                        TaskExecution.create(
                            name="t", implementing_class="noop_mark_once", stage_start=True, stage_end=True
                        )
                    ],
                )
            ],
        )
        store.store(workflow)
        inserts = _count_processed_inserts(monkeypatch)
        Orchestrator(queue).start(workflow)
        handled = processor.process_all(timeout=30.0)

        assert store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED
        assert handled >= 7
        assert len(inserts) == handled

        with store._pool.connection() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM processed_messages WHERE execution_id = %s", (workflow.id,)
            ).fetchone()
        assert row["n"] == handled
    finally:
        queue.close()
        store.close()
