"""WI-4 slice 4b: the task generation fence and complete resets (jump, restart, multi-merge re-arm)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import JoinType, StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import CompleteTask, JumpToStage
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.stages.builder import StageDefinitionBuilder, get_default_factory
from stabilize.tasks.interface import RetryableTask, Task
from stabilize.tasks.result import TaskResult
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mggen") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'gen.db'}"
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


class _Record(Task):
    runs: list[str] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        _Record.runs.append(stage.ref_id)
        return TaskResult.success(outputs={f"{stage.ref_id}_runs": _Record.runs.count(stage.ref_id)})


class _RunningOnceThenDone(RetryableTask):
    calls: list[str] = []

    def get_timeout(self) -> timedelta:
        return timedelta(minutes=1)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(seconds=2)

    def execute(self, stage: StageExecution) -> TaskResult:
        _RunningOnceThenDone.calls.append(stage.ref_id)
        if len(_RunningOnceThenDone.calls) == 1:
            return TaskResult.running()
        return TaskResult.success(outputs={"b_runs": len(_RunningOnceThenDone.calls)})


class _SlowNumbered(Task):
    calls = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        _SlowNumbered.calls += 1
        run = _SlowNumbered.calls
        time.sleep(1.5)
        return TaskResult.success(outputs={"run": run})


class _JumpOnce(Task):
    jumped: list[str] = []
    target = "stage_b"

    def execute(self, stage: StageExecution) -> TaskResult:
        if not _JumpOnce.jumped:
            _JumpOnce.jumped.append(stage.ref_id)
            return TaskResult.jump_to(_JumpOnce.target)
        return TaskResult.success()


class _FailOnceThenSucceed(Task):
    calls = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        _FailOnceThenSucceed.calls += 1
        if _FailOnceThenSucceed.calls == 1:
            return TaskResult.terminal("first attempt fails")
        return TaskResult.success()


class _RollbackBuilder(StageDefinitionBuilder):
    @property
    def type(self) -> str:
        return "onfail_restart_demo"

    def on_failure_stages(self, stage: StageExecution, graph: Any) -> None:
        graph.add(
            StageExecution(
                ref_id=f"{stage.ref_id}_rollback",
                type="noop",
                name="Rollback",
                tasks=[
                    TaskExecution.create(name="rb", implementing_class="record_gen", stage_start=True, stage_end=True)
                ],
            )
        )


def _task(task_type: str) -> TaskExecution:
    return TaskExecution.create(name="t", implementing_class=task_type, stage_start=True, stage_end=True)


def _stage(ref_id: str, task_type: str, **extra: Any) -> StageExecution:
    extra.setdefault("type", task_type)
    return StageExecution(ref_id=ref_id, name=ref_id.upper(), tasks=[_task(task_type)], **extra)


def _processor(store: Any, queue: Any, **config: Any) -> Any:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.registry import TaskRegistry

    registry = TaskRegistry()
    registry.register("record_gen", _Record)
    registry.register("running_once", _RunningOnceThenDone)
    registry.register("slow_numbered", _SlowNumbered)
    registry.register("jump_once", _JumpOnce)
    registry.register("fail_once", _FailOnceThenSucceed)
    return QueueProcessor(
        queue,
        store=store,
        task_registry=registry,
        config=QueueProcessorConfig(**config),
        handler_config=HandlerConfig(handler_retry_delay_seconds=0.1),
    )


def _reset_counters() -> None:
    _Record.runs.clear()
    _RunningOnceThenDone.calls.clear()
    _SlowNumbered.calls = 0
    _JumpOnce.jumped.clear()
    _FailOnceThenSucceed.calls = 0


def test_generation_round_trips_and_defaults_to_zero(any_db: str) -> None:
    store, queue = _parts(any_db)
    try:
        wf = Workflow.create(application="gen", name="w", stages=[_stage("s0", "record_gen")])
        store.store(wf)
        stage = store.retrieve_stage(wf.stages[0].id)
        assert stage.tasks[0].generation == 0
        stage.tasks[0].generation = 3
        store.store_stage(stage)
        assert store.retrieve_stage(stage.id).tasks[0].generation == 3
        with store.transaction(queue) as txn:
            fresh = store.retrieve_stage(stage.id)
            fresh.tasks[0].generation = 4
            txn.store_stage(fresh)
        assert store.retrieve_stage(stage.id).tasks[0].generation == 4
    finally:
        queue.close()
        store.close()


def test_reset_for_retry_bumps_the_generation_and_clears_every_reset_key() -> None:
    from stabilize.handlers.jump_to_stage.reset import reset_stage_for_retry
    from stabilize.tasks.registry import TaskRegistry

    stage = _stage("w", "WaitTask", context={"waitTime": 1})
    stage.status = WorkflowStatus.SUCCEEDED
    stage.scheduled_time = 5
    stage.context.update(
        {
            "_timer_armed_at": 1,
            "_timer_due_at": 2,
            "waitStartTime": 3,
            "_paused_at": 4,
            "_paused_ms": 5,
            "exception": {"details": {"error": "old"}},
            "_on_failure_planned": True,
            "_buffered_signals": [{"signal_name": "x"}],
            "_signal_name": "x",
            "keep_me": "yes",
        }
    )
    task = stage.tasks[0]
    task.status = WorkflowStatus.SUCCEEDED
    task.generation = 2
    reset_stage_for_retry(stage, task_registry=TaskRegistry())
    assert task.generation == 3
    assert task.status == WorkflowStatus.NOT_STARTED
    assert stage.scheduled_time is None
    for key in (
        "_timer_armed_at",
        "_timer_due_at",
        "waitStartTime",
        "_paused_at",
        "_paused_ms",
        "exception",
        "_on_failure_planned",
        "_buffered_signals",
        "_signal_name",
    ):
        assert key not in stage.context, key
    assert stage.context["keep_me"] == "yes" and stage.context["waitTime"] == 1


def test_a_backward_jump_leaves_no_stale_run_task_deliverable(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset_counters()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="gen",
            name="jump",
            stages=[
                _stage("stage_a", "record_gen"),
                _stage("stage_b", "running_once", requisite_stage_ref_ids={"stage_a"}),
                _stage("stage_d", "jump_once", requisite_stage_ref_ids={"stage_a"}),
                _stage("stage_c", "record_gen", requisite_stage_ref_ids={"stage_b", "stage_d"}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        b_task_id = next(s for s in wf.stages if s.ref_id == "stage_b").tasks[0].id
        for _ in range(80):
            message = queue.poll_one()
            assert message is not None, "the jump never arrived"
            processor._handle_message(message)
            queue.ack(message)
            if isinstance(message, JumpToStage):
                break
        assert not queue.has_pending_message_for_task(b_task_id), "the previous generation's RunTask is still queued"
        processor.process_all(timeout=20)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.SUCCEEDED, done.status
        assert _RunningOnceThenDone.calls == ["stage_b", "stage_b"], (
            f"a stale RunTask of the previous generation ran: {_RunningOnceThenDone.calls}"
        )
        assert _Record.runs.count("stage_c") == 1
        b = next(s for s in done.stages if s.ref_id == "stage_b")
        assert b.tasks[0].generation == 1
        assert queue.next_ready_in() is None
    finally:
        queue.close()
        store.close()


def test_a_result_from_the_previous_generation_is_dropped_at_save_time(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset_counters()
    store, queue = _parts(db)
    processor = _processor(store, queue, poll_frequency_ms=50)
    try:
        wf = Workflow.create(
            application="gen",
            name="stale-save",
            stages=[
                _stage("stage_a", "record_gen"),
                _stage("stage_b", "slow_numbered", requisite_stage_ref_ids={"stage_a"}),
                _stage("stage_d", "jump_once", requisite_stage_ref_ids={"stage_a"}),
                _stage("stage_c", "record_gen", requisite_stage_ref_ids={"stage_b", "stage_d"}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        processor.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if store.retrieve(wf.id).status.is_complete and queue.next_ready_in() is None:
                break
            time.sleep(0.1)
        processor.stop()
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.SUCCEEDED, done.status
        b = next(s for s in done.stages if s.ref_id == "stage_b")
        assert _SlowNumbered.calls == 2, f"stage_b ran {_SlowNumbered.calls} times"
        assert b.outputs["run"] == 2, f"the stale generation's result survived: {b.outputs}"
        assert _Record.runs.count("stage_c") == 1
    finally:
        queue.close()
        store.close()


def test_a_jump_releases_the_claims_of_the_stages_it_resets(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset_counters()
    _JumpOnce.target = "entry"
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="gen",
            name="claims",
            stages=[
                _stage("entry", "record_gen"),
                _stage("m", "record_gen", requisite_stage_ref_ids={"entry"}, mutex_key="one-at-a-time"),
                _stage("j", "jump_once", requisite_stage_ref_ids={"m"}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        m_id = next(s.id for s in wf.stages if s.ref_id == "m")
        jumped = False
        for _ in range(60):
            message = queue.poll_one()
            assert message is not None, "the jump never arrived"
            processor._handle_message(message)
            queue.ack(message)
            if isinstance(message, JumpToStage):
                jumped = True
                break
        assert jumped
        with store.transaction(queue) as txn:
            assert txn.claim_owner(wf.id, "mutex:one-at-a-time") is None, "the mutex claim survived the reset"
        processor.process_all(timeout=20)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.SUCCEEDED
        assert _Record.runs.count("m") == 2
        with store.transaction(queue) as txn:
            assert txn.claim_owner(wf.id, "mutex:one-at-a-time") == m_id
    finally:
        _JumpOnce.target = "stage_b"
        queue.close()
        store.close()


def test_a_multi_merge_re_arm_bumps_the_generation(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset_counters()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="gen",
            name="mm",
            stages=[
                _stage("a", "record_gen"),
                _stage("b", "record_gen"),
                _stage("mm", "record_gen", requisite_stage_ref_ids={"a", "b"}, join_type=JoinType.MULTI_MERGE),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        processor.process_all(timeout=20)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.SUCCEEDED
        mm = next(s for s in done.stages if s.ref_id == "mm")
        assert _Record.runs.count("mm") == 2
        assert mm.tasks[0].generation == 1, "the re-arm did not bump the task generation"
    finally:
        queue.close()
        store.close()


def test_restart_drops_the_old_on_failure_child_and_clears_the_failure_keys(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset_counters()
    get_default_factory().register(_RollbackBuilder())
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="gen",
            name="restart",
            stages=[_stage("p", "fail_once", type="onfail_restart_demo")],
        )
        orchestrator = Orchestrator(queue, store=store)
        orchestrator.start(wf)
        processor.process_all(timeout=20)
        failed = store.retrieve(wf.id)
        assert failed.status == WorkflowStatus.TERMINAL
        parent = next(s for s in failed.stages if s.ref_id == "p")
        assert _Record.runs.count("p_rollback") == 1
        assert parent.context.get("_on_failure_planned") is True

        orchestrator.restart(failed, parent.id)
        processor.process_all(timeout=20)
        retried = store.retrieve(wf.id)
        assert retried.status == WorkflowStatus.SUCCEEDED, retried.status
        parent = next(s for s in retried.stages if s.ref_id == "p")
        assert "_on_failure_planned" not in parent.context
        assert "exception" not in parent.context
        assert [s.ref_id for s in retried.stages if s.parent_stage_id == parent.id] == []
        assert parent.tasks[0].generation == 1
    finally:
        queue.close()
        store.close()


def test_complete_task_with_a_stale_generation_is_ignored(db: str) -> None:
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(application="gen", name="ct", stages=[_stage("s0", "record_gen")])
        store.store(wf)
        stage = store.retrieve_stage(wf.stages[0].id)
        stage.status = WorkflowStatus.RUNNING
        stage.start_time = int(time.time() * 1000)
        task = stage.tasks[0]
        task.status = WorkflowStatus.RUNNING
        task.start_time = stage.start_time
        task.generation = 1
        store.store_stage(stage)
        execution = store.retrieve(wf.id)
        execution.status = WorkflowStatus.RUNNING
        store.update_status(execution)
        queue.push(
            CompleteTask(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=stage.id,
                task_id=task.id,
                status=WorkflowStatus.SUCCEEDED,
                generation=0,
            )
        )
        assert processor.process_one() is True
        assert store.retrieve_stage(stage.id).tasks[0].status == WorkflowStatus.RUNNING
        queue.push(
            CompleteTask(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=stage.id,
                task_id=task.id,
                status=WorkflowStatus.SUCCEEDED,
                generation=1,
            )
        )
        assert processor.process_one() is True
        assert store.retrieve_stage(stage.id).tasks[0].status == WorkflowStatus.SUCCEEDED
    finally:
        queue.close()
        store.close()
