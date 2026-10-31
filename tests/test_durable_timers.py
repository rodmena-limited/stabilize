"""WI-4 slice 4a: durable timers, scheduled_time on updates, and a drain that sleeps until the next message."""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import PauseTask, ResumeStage, RunTask, StartWorkflow
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.queue.processor.handler_base import MessageHandler
from stabilize.tasks.interface import RetryableTask
from stabilize.tasks.result import TaskResult
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgtimer") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'timers.db'}"
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


class _CountedTimer(RetryableTask):
    calls: list[float] = []

    def __new__(cls) -> _CountedTimer:
        from stabilize.tasks.interface import TimerTask

        class Counted(TimerTask):
            def execute(self, stage: StageExecution) -> TaskResult:
                _CountedTimer.calls.append(time.monotonic())
                return super().execute(stage)

        return Counted()  # type: ignore[return-value]

    def get_timeout(self) -> timedelta:
        return timedelta(minutes=1)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class _CountedWait(RetryableTask):
    calls: list[float] = []

    def __new__(cls) -> _CountedWait:
        from stabilize.tasks.interface import WaitTask

        class Counted(WaitTask):
            def execute(self, stage: StageExecution) -> TaskResult:
                _CountedWait.calls.append(time.monotonic())
                return super().execute(stage)

        return Counted()  # type: ignore[return-value]

    def get_timeout(self) -> timedelta:
        return timedelta(minutes=1)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class _TenMinutePoll(RetryableTask):
    executed: list[str] = []

    def get_timeout(self) -> timedelta:
        return timedelta(minutes=10)

    def execute(self, stage: StageExecution) -> TaskResult:
        _TenMinutePoll.executed.append(stage.id)
        return TaskResult.success()


class _Boom(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=1)

    def execute(self, stage: StageExecution) -> TaskResult:
        raise RuntimeError("boom")


class _RaisingStart(MessageHandler[StartWorkflow]):
    @property
    def message_type(self) -> type[StartWorkflow]:
        return StartWorkflow

    def handle(self, message: StartWorkflow) -> None:
        raise RuntimeError("handler boom")


def _processor(store: Any, queue: Any, timers: bool = False, **config: Any) -> Any:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.registry import TaskRegistry

    registry = TaskRegistry()
    if timers:
        registry.register("timer_counted", _CountedTimer)
        registry.register("wait_counted", _CountedWait)
    registry.register("ten_minute_poll", _TenMinutePoll)
    registry.register("boom_task", _Boom)
    return QueueProcessor(
        queue,
        store=store,
        task_registry=registry,
        config=QueueProcessorConfig(**config),
        handler_config=HandlerConfig(handler_retry_delay_seconds=0.1),
    )


def _one_stage(task_type: str, context: dict[str, Any]) -> Workflow:
    task = TaskExecution.create(name="t", implementing_class=task_type, stage_start=True, stage_end=True)
    stage = StageExecution(ref_id="s0", type=task_type, name="S0", tasks=[task], context=context)
    return Workflow.create(application="timers", name="w", stages=[stage])


def test_process_all_returns_at_once_when_the_only_message_is_an_hour_away(any_db: str) -> None:
    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        queue.push(
            StartWorkflow(execution_type="PIPELINE", execution_id="01FAR0000000000000000001"), timedelta(hours=1)
        )
        started = time.monotonic()
        assert processor.process_all(timeout=5) == 0
        assert time.monotonic() - started < 0.5, "process_all spun for its timeout with one far-future message"
        assert processor.last_drain.reason == "next_ready_beyond_budget"
    finally:
        queue.close()
        store.close()


def test_next_ready_in_reports_the_earliest_pollable_message(any_db: str) -> None:
    store, queue = _parts(any_db)
    try:
        assert queue.next_ready_in() is None
        queue.push(
            StartWorkflow(execution_type="PIPELINE", execution_id="01NEXT000000000000000001"), timedelta(seconds=2)
        )
        wait = queue.next_ready_in()
        assert wait is not None and 1.0 < wait <= 2.01, wait
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01NEXT000000000000000002"))
        assert queue.next_ready_in() == 0
        leased = queue.poll_one()
        assert leased is not None
        wait = queue.next_ready_in()
        assert wait is not None and 0.5 < wait <= 2.01, "a leased message counted as ready"
        queue.ack(leased)
        time.sleep(2.1)
        due_now = queue.poll_one()
        assert due_now is not None
        queue.ack(due_now)
        assert queue.next_ready_in() is None
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01NEXT000000000000000003"))
        for _ in range(queue.max_attempts + 1):
            poisoned = queue.poll_one()
            if poisoned is None:
                break
            queue.reschedule(poisoned, timedelta(0))
        assert queue.size() == 1, "the poisoned row is still in the table"
        assert queue.next_ready_in() is None, "a row past max_attempts counted as pollable"
    finally:
        queue.close()
        store.close()


def test_timer_task_arms_once_and_fires_at_its_due_time(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _CountedTimer.calls.clear()
    store, queue = _parts(db)
    processor = _processor(store, queue, timers=True)
    try:
        wf = _one_stage("timer_counted", {"duration_seconds": 1.5})
        Orchestrator(queue, store=store).start(wf)
        while True:
            message = queue.poll_one()
            assert message is not None
            processor._handle_message(message)
            queue.ack(message)
            if isinstance(message, RunTask):
                break
        armed = store.retrieve_stage(wf.stages[0].id)
        due = armed.context["_timer_due_at"]
        assert isinstance(due, int) and due > armed.context["_timer_armed_at"]
        assert armed.scheduled_time == due, "scheduled_time was not written by the stage update"
        wait = queue.next_ready_in()
        assert wait is not None and 1.0 < wait <= 1.5, f"the re-dispatch is not scheduled at the due time: {wait}"
        started = time.monotonic()
        processor.process_all(timeout=30)
        finished = store.retrieve(wf.id)
        assert finished.status == WorkflowStatus.SUCCEEDED
        assert time.monotonic() - started >= 1.0
        assert len(_CountedTimer.calls) == 2, (
            f"a timer executes to arm and to fire, not {len(_CountedTimer.calls)} times"
        )
        assert finished.stages[0].scheduled_time is None
    finally:
        queue.close()
        store.close()


def test_timer_until_in_the_past_fires_at_once_and_a_bad_until_is_refused(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    store, queue = _parts(db)
    processor = _processor(store, queue, timers=True)
    try:
        past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        prompt = _one_stage("timer_counted", {"until": past})
        broken = _one_stage("timer_counted", {"until": "yesterday"})
        both = _one_stage("timer_counted", {"until": past, "duration_seconds": 3})
        orchestrator = Orchestrator(queue, store=store)
        for wf in (prompt, broken, both):
            orchestrator.start(wf)
        processor.process_all(timeout=30)
        assert store.retrieve(prompt.id).status == WorkflowStatus.SUCCEEDED
        refused = store.retrieve(broken.id)
        assert refused.status == WorkflowStatus.TERMINAL
        assert "until" in str(refused.stages[0].context.get("exception"))
        assert store.retrieve(both.id).status == WorkflowStatus.TERMINAL
    finally:
        queue.close()
        store.close()


def test_wait_task_is_due_based_and_keeps_its_contract_keys(db: str) -> None:
    from stabilize.orchestrator import Orchestrator
    from stabilize.tasks.interface import WaitTask

    _CountedWait.calls.clear()
    store, queue = _parts(db)
    processor = _processor(store, queue, timers=True)
    try:
        wf = _one_stage("wait_counted", {"waitTime": 1})
        Orchestrator(queue, store=store).start(wf)
        processor.process_all(timeout=30)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.SUCCEEDED
        stage = done.stages[0]
        assert stage.outputs["waitedSeconds"] >= 1
        assert stage.context["_timer_due_at"] == stage.context["waitStartTime"] * 1000 + 1000
        assert len(_CountedWait.calls) == 2, f"WaitTask polled {len(_CountedWait.calls)} times for a 1 s wait"
    finally:
        queue.close()
        store.close()
    two_days = StageExecution(ref_id="x", type="wait", name="X", context={"waitTime": 172800})
    two_days.context["_timer_armed_at"] = 1_000
    two_days.context["_timer_due_at"] = 1_000 + 172800 * 1000
    assert WaitTask().get_dynamic_timeout(two_days) >= timedelta(hours=48), "WaitTask is still capped at 24 h"
    assert "waitStartTime" in WaitTask().reset_context_keys


def test_scheduled_time_is_written_by_the_stage_update(any_db: str) -> None:
    store, queue = _parts(any_db)
    try:
        wf = _one_stage("ten_minute_poll", {})
        store.store(wf)
        stage = store.retrieve_stage(wf.stages[0].id)
        stage.scheduled_time = 1_700_000_000_000
        store.store_stage(stage)
        assert store.retrieve_stage(stage.id).scheduled_time == 1_700_000_000_000
        again = store.retrieve_stage(stage.id)
        again.scheduled_time = None
        with store.transaction(queue) as txn:
            txn.store_stage(again)
        assert store.retrieve_stage(stage.id).scheduled_time is None
    finally:
        queue.close()
        store.close()


def test_merge_outputs_and_the_builder_keep_every_field() -> None:
    first = TaskResult(status=WorkflowStatus.REDIRECT, outputs={"a": 1}, target_stage_ref_id="target", scheduled_time=5)
    merged = first.merge_outputs(TaskResult.success(outputs={"b": 2}))
    assert merged.target_stage_ref_id == "target"
    assert merged.scheduled_time == 5
    assert merged.outputs == {"a": 1, "b": 2}
    built = TaskResult.builder(WorkflowStatus.RUNNING).scheduled_time(9).build()
    assert built.scheduled_time == 9
    assert TaskResult.running(scheduled_time=7).scheduled_time == 7


def test_a_paused_interval_does_not_count_against_the_retryable_lifetime(db: str) -> None:
    _TenMinutePoll.executed.clear()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = _one_stage("ten_minute_poll", {})
        store.store(wf)
        stage = store.retrieve_stage(wf.stages[0].id)
        now_ms = int(time.time() * 1000)
        stage.status = WorkflowStatus.RUNNING
        stage.start_time = now_ms - 65 * 60_000
        stage.context["_paused_ms"] = 60 * 60_000
        task = stage.tasks[0]
        task.status = WorkflowStatus.RUNNING
        task.start_time = now_ms - 65 * 60_000
        store.store_stage(stage)
        execution = store.retrieve(wf.id)
        execution.status = WorkflowStatus.RUNNING
        store.update_status(execution)
        queue.push(
            RunTask(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=stage.id,
                task_id=task.id,
                task_type="ten_minute_poll",
            )
        )
        assert processor.process_one() is True
        assert _TenMinutePoll.executed == [stage.id], "the paused hour was counted and the task timed out"
    finally:
        queue.close()
        store.close()


def test_pause_and_resume_record_the_paused_interval(db: str) -> None:
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = _one_stage("ten_minute_poll", {})
        store.store(wf)
        stage = store.retrieve_stage(wf.stages[0].id)
        stage.status = WorkflowStatus.RUNNING
        stage.start_time = int(time.time() * 1000)
        stage.tasks[0].status = WorkflowStatus.RUNNING
        stage.tasks[0].start_time = stage.start_time
        store.store_stage(stage)
        execution = store.retrieve(wf.id)
        execution.status = WorkflowStatus.PAUSED
        store.update_status(execution)
        queue.push(
            PauseTask(execution_type="PIPELINE", execution_id=wf.id, stage_id=stage.id, task_id=stage.tasks[0].id)
        )
        assert processor.process_one() is True
        paused = store.retrieve_stage(stage.id)
        assert paused.status == WorkflowStatus.PAUSED
        assert isinstance(paused.context.get("_paused_at"), int)
        time.sleep(0.3)
        queue.push(ResumeStage(execution_type="PIPELINE", execution_id=wf.id, stage_id=stage.id))
        assert processor.process_one() is True
        resumed = store.retrieve_stage(stage.id)
        assert resumed.status == WorkflowStatus.RUNNING
        assert "_paused_at" not in resumed.context
        assert resumed.context["_paused_ms"] >= 300
    finally:
        queue.close()
        store.close()


def test_process_all_can_drain_through_handler_errors(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    store, queue = _parts(db)
    processor = _processor(store, queue, retry_delay=timedelta(hours=1))
    try:
        wf = _one_stage("boom_task", {})
        Orchestrator(queue, store=store).start(wf)
        processed = processor.process_all(timeout=10, raise_on_error=False)
        assert processed >= 3
        assert processor.last_drain.errors == 0, "a task exception is handled by RunTask, not raised to the drain"
        assert store.retrieve(wf.id).status in (WorkflowStatus.TERMINAL, WorkflowStatus.RUNNING)
        processor.replace_handler(_RaisingStart())
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01ERR0000000000000000001"))
        processor.process_all(timeout=5, raise_on_error=False)
        assert processor.last_drain.errors == 1
        assert processor.last_drain.reason in ("empty", "next_ready_beyond_budget")
        queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="01ERR0000000000000000002"))
        with pytest.raises(RuntimeError, match="handler boom"):
            processor.process_all(timeout=5)
    finally:
        queue.close()
        store.close()
