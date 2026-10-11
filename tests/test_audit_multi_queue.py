from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from audit_support import drive, make_processor, stage

from stabilize import Orchestrator, PostgresQueue, SqliteQueue, StageExecution, TaskResult, Workflow
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import RetryableTask, Task

COUNT: dict[str, int] = {}


class Count(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        COUNT[stage.execution.id] = COUNT.get(stage.execution.id, 0) + 1
        return TaskResult.success()


class Poll(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(minutes=1)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()


@pytest.fixture
def second_queue(queue: Queue, backend: str) -> Iterator[Any]:
    table = f"q_{uuid.uuid4().hex[:8]}"
    if backend == "sqlite":
        q = SqliteQueue(queue.connection_string, table_name=table)
        q._create_table()
        yield q
        q.close()
        return
    import psycopg

    with psycopg.connect(queue.connection_string, autocommit=True) as conn:
        conn.execute(f"CREATE TABLE {table} (LIKE queue_messages INCLUDING ALL)")
        conn.execute(f"CREATE TABLE {table}_dlq (LIKE queue_messages_dlq INCLUDING ALL)")
        conn.execute(f"CREATE SEQUENCE {table}_id_seq AS BIGINT OWNED BY {table}.id")
        conn.execute(f"ALTER TABLE {table} ALTER COLUMN id SET DEFAULT nextval('{table}_id_seq')")
    q = PostgresQueue(queue.connection_string, table_name=table)
    yield q
    q.close()
    with psycopg.connect(queue.connection_string, autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table}_dlq, {table}")


def _wf(app: str, impl: str) -> Workflow:
    return Workflow.create(application=app, name=app, stages=[stage("s", [impl])])


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    COUNT.clear()


def test_every_message_on_two_tables_is_handled(repository: WorkflowStore, queue: Queue, second_queue: Any) -> None:
    pa = make_processor(repository, queue, {"count": Count})
    pb = make_processor(repository, second_queue, {"count": Count})
    wa, wb = _wf("app-a", "count"), _wf("app-b", "count")
    for wf, q in ((wa, queue), (wb, second_queue)):
        repository.store(wf)
        Orchestrator(q, store=repository).start(wf)
    for _ in range(3):
        drive(pa, repository, wa.id, idle=0.3)
        drive(pb, repository, wb.id, idle=0.3)
    ra, rb = repository.retrieve(wa.id), repository.retrieve(wb.id)
    assert (ra.status, rb.status) == (WorkflowStatus.SUCCEEDED, WorkflowStatus.SUCCEEDED), (
        f"C1/C6 WA={ra.status.name} WB={rb.status.name}"
    )
    assert COUNT.get(wa.id) == 1 and COUNT.get(wb.id) == 1


def test_unfiltered_recovery_does_not_reissue_work_owned_by_another_table(
    repository: WorkflowStore, queue: Queue, second_queue: Any
) -> None:
    pb = make_processor(repository, second_queue, {"poll": Poll})
    pa = make_processor(repository, queue, {"poll": Poll})
    pa.config.recovery_application = None
    wb = _wf(f"app-b-{uuid.uuid4().hex[:6]}", "poll")
    repository.store(wb)
    Orchestrator(second_queue, store=repository).start(wb)
    drive(pb, repository, wb.id, idle=0.5)
    task_id = repository.retrieve(wb.id).stages[0].tasks[0].id
    assert second_queue.has_pending_message_for_task(task_id)
    pa.run_recovery()
    assert not queue.has_pending_message_for_task(task_id), "C31 recovery pushed a RunTask for another table's task"
