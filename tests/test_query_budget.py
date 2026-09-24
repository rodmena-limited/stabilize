"""The number of SQL statements to run a no-op stage on PostgreSQL is bounded (#46).

The bound is the measured count on the release that introduced it. A change
that raises it fails here and has to move the number deliberately; see
docs/guide/query_budget.rst for the per-statement breakdown.
"""

from __future__ import annotations

from typing import Any

import pytest

from stabilize import Orchestrator, QueueProcessor, Task, TaskRegistry, TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

ONE_STAGE_BUDGET = 87
PER_EXTRA_STAGE_BUDGET = 67


class _NoOp(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def _statements_for(postgres_url: str, stages: int, monkeypatch: pytest.MonkeyPatch) -> int:
    import psycopg

    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue import PostgresQueue

    store = PostgresWorkflowStore(postgres_url)
    queue = PostgresQueue(postgres_url, table_name="queue_messages")
    counted: list[int] = []
    real = psycopg.Cursor.execute

    def execute(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        counted.append(1)
        return real(self, query, *args, **kwargs)

    try:
        queue.clear()
        registry = TaskRegistry()
        registry.register("noop_budget", _NoOp)
        processor = QueueProcessor(queue, store=store, task_registry=registry)
        stage_list = [
            StageExecution(
                ref_id=f"s{i}",
                type="noop_budget",
                name=f"S{i}",
                requisite_stage_ref_ids=set() if i == 0 else {f"s{i - 1}"},
                tasks=[
                    TaskExecution.create(name="t", implementing_class="noop_budget", stage_start=True, stage_end=True)
                ],
            )
            for i in range(stages)
        ]
        workflow = Workflow.create(application="budget", name=f"n{stages}", stages=stage_list)
        monkeypatch.setattr(psycopg.Cursor, "execute", execute)
        store.store(workflow)
        Orchestrator(queue).start(workflow)
        processor.process_all(timeout=30.0)
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
        assert store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED
        return len(counted)
    finally:
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
        queue.close()
        store.close()


def test_the_counter_sees_a_deliberate_statement(postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import psycopg

    counted: list[int] = []
    real = psycopg.Cursor.execute

    def execute(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        counted.append(1)
        return real(self, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", execute)
    with psycopg.connect(postgres_url) as conn:
        conn.execute("SELECT 1")
    assert len(counted) == 1


def test_one_stage_stays_within_budget(postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    assert _statements_for(postgres_url, 1, monkeypatch) <= ONE_STAGE_BUDGET


def test_each_extra_stage_stays_within_budget(postgres_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    two = _statements_for(postgres_url, 2, monkeypatch)
    four = _statements_for(postgres_url, 4, monkeypatch)
    assert (four - two) / 2 <= PER_EXTRA_STAGE_BUDGET
