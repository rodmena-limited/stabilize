"""A fence refusal latches the processor: it stops, keeps the message unacked, and refuses to start again."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.tenancy import TenancyModeChanged, tenant_error_from
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from tests.mg_support import output, scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


class _NoOp(Task):
    def execute(self, stage: Any) -> TaskResult:
        return TaskResult.success()


@pytest.fixture()
def tenancy_db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mglatch") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        enabled = stabilize("mg-tenancy", "enable", "--db-url", url)
        assert enabled.returncode == 0, output(enabled)
        yield url


def _workflow() -> Workflow:
    return Workflow.create(
        application="latch",
        name="w",
        stages=[
            StageExecution(
                ref_id="s0",
                type="noop_latch",
                name="S0",
                tasks=[
                    TaskExecution.create(name="t", implementing_class="noop_latch", stage_start=True, stage_end=True)
                ],
            )
        ],
    )


def _wait_until_stopped(processor: Any, seconds: float = 20.0) -> None:
    deadline = time.monotonic() + seconds
    while processor.is_running and time.monotonic() < deadline:
        time.sleep(0.05)


def test_a_fence_refusal_inside_a_handler_latches_the_processor_and_leaves_the_message(
    tenancy_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.orchestrator import Orchestrator
    from stabilize.persistence.pool_options import PoolOptions
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue
    from stabilize.queue.processor import QueueProcessor
    from stabilize.tasks.registry import TaskRegistry

    store = PostgresWorkflowStore(tenancy_db, tenant="acme", options=PoolOptions(min_size=0, max_size=3))
    queue = PostgresQueue(tenancy_db, tenant="acme")
    registry = TaskRegistry()
    registry.register("noop_latch", _NoOp)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    try:
        Orchestrator(queue, store=store).start(_workflow())

        @contextmanager
        def unarmed_borrow() -> Iterator[Any]:
            with store._pool.connection() as conn:
                try:
                    yield conn
                except Exception as exc:
                    mapped = tenant_error_from(exc)
                    if mapped is not None:
                        raise mapped from exc
                    raise

        monkeypatch.setattr(store, "_borrow", unarmed_borrow)
        processor.start()
        _wait_until_stopped(processor)
        assert not processor.is_running, "the processor kept running after the fence refused its writes"
        assert isinstance(processor.fault, TenancyModeChanged), processor.fault
        processor.stop()
        monkeypatch.undo()

        assert queue.size() == 1, "the message was acked or lost"
        assert queue.poll_one() is None, "the message was rescheduled instead of keeping its lease"
        with pytest.raises(TenancyModeChanged):
            processor.start()
    finally:
        processor.stop()
        queue.close()
        store.close()


def test_a_fence_refusal_in_the_poll_loop_latches_the_processor(
    tenancy_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.orchestrator import Orchestrator
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue
    from stabilize.queue.processor import QueueProcessor
    from stabilize.tasks.registry import TaskRegistry

    store = PostgresWorkflowStore(tenancy_db, tenant="acme")
    queue = PostgresQueue(tenancy_db, tenant="acme")
    registry = TaskRegistry()
    registry.register("noop_latch", _NoOp)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    try:
        Orchestrator(queue, store=store).start(_workflow())

        def unarmed(conn: Any, tenant: str, setting: str | None, reader: Any = None) -> str:
            if reader is not None:
                reader.record("tenancy")
            return "tenancy"

        monkeypatch.setattr("stabilize.persistence.tenancy.arm_connection", unarmed)
        processor.start()
        _wait_until_stopped(processor)
        assert not processor.is_running
        assert isinstance(processor.fault, TenancyModeChanged), processor.fault
        processor.stop()
        monkeypatch.undo()
        assert queue.size() == 1
    finally:
        processor.stop()
        queue.close()
        store.close()
