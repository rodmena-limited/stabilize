"""WI-4 slice 4d: SubWorkflowTask on the processor's own store and queue, and process isolation that keeps errors."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest

from stabilize.errors import TransientError
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgsub") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


def _parts(url: str) -> tuple[Any, Any]:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    return PostgresWorkflowStore(url), PostgresQueue(url)


class _Record(Task):
    runs: list[str] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        _Record.runs.append(stage.ref_id)
        return TaskResult.success(outputs={"seen": stage.ref_id})


class _JumpOnce(Task):
    jumped: list[str] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        if not _JumpOnce.jumped:
            _JumpOnce.jumped.append(stage.ref_id)
            return TaskResult.jump_to(stage.context["jump_target"])
        return TaskResult.success()


def _task(task_type: str) -> TaskExecution:
    return TaskExecution.create(name="t", implementing_class=task_type, stage_start=True, stage_end=True)


def _stage(ref_id: str, task_type: str, **extra: Any) -> StageExecution:
    extra.setdefault("type", task_type)
    return StageExecution(ref_id=ref_id, name=ref_id.upper(), tasks=[_task(task_type)], **extra)


def _child_config(name: str = "child", task_type: str = "record_sub", context: dict[str, Any] | None = None) -> dict:
    from stabilize.models.stage.definition import stage_to_definition

    return {
        "application": "child-app",
        "name": name,
        "stages": [stage_to_definition(_stage("c0", task_type, context=context or {}))],
    }


def _processor(store: Any, queue: Any, **config: Any) -> Any:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.registry import TaskRegistry
    from stabilize.tasks.sub_workflow import SubWorkflowTask

    registry = TaskRegistry()
    registry.register("record_sub", _Record)
    registry.register("jump_once_sub", _JumpOnce)
    registry.register("SubWorkflowTask", SubWorkflowTask)
    return QueueProcessor(
        queue,
        store=store,
        task_registry=registry,
        config=QueueProcessorConfig(**config),
        handler_config=HandlerConfig(handler_retry_delay_seconds=0.1),
    )


def _reset() -> None:
    _Record.runs.clear()
    _JumpOnce.jumped.clear()
    from stabilize.orchestrator import Orchestrator

    Orchestrator._instance = None


def test_sub_workflow_runs_on_the_processors_store_and_queue_without_the_global_orchestrator(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        parent = Workflow.create(
            application="parent-app",
            name="parent",
            stages=[_stage("sub", "SubWorkflowTask", context={"_sub_workflow_config": _child_config()})],
        )
        Orchestrator(queue, store=store).start(parent)
        Orchestrator._instance = None
        processor.process_all(timeout=30)
        done = store.retrieve(parent.id)
        assert done.status == WorkflowStatus.SUCCEEDED, done.status
        sub = done.stages[0]
        child_id = sub.outputs.get("sub_workflow_id")
        assert child_id, sub.outputs
        child = store.retrieve(child_id)
        assert child.status == WorkflowStatus.SUCCEEDED
        assert child.context.get("_parent_workflow_id") == parent.id
        assert _Record.runs == ["c0"]
        assert store.count_by_application("child-app") == 1
    finally:
        queue.close()
        store.close()


def test_the_child_id_is_deterministic_so_a_second_start_reuses_the_child(db: str) -> None:
    from stabilize.tasks.handles import bind_engine
    from stabilize.tasks.sub_workflow import SubWorkflowTask

    _reset()
    store, queue = _parts(db)
    try:
        parent = Workflow.create(
            application="parent-app",
            name="parent",
            stages=[_stage("sub", "SubWorkflowTask", context={"_sub_workflow_config": _child_config()})],
        )
        store.store(parent)
        stage = store.retrieve_stage(parent.stages[0].id)
        stage.status = WorkflowStatus.RUNNING
        stage.start_time = int(time.time() * 1000)
        stage.tasks[0].status = WorkflowStatus.RUNNING
        stage.tasks[0].start_time = stage.start_time
        store.store_stage(stage)
        fresh = store.retrieve(parent.id)
        running = next(s for s in fresh.stages if s.ref_id == "sub")
        with bind_engine(store, queue):
            first = SubWorkflowTask().execute(running)
            assert first.status == WorkflowStatus.RUNNING
            child_id = first.context["_sub_workflow_id"]
            running.context.pop("_sub_workflow_id", None)
            second = SubWorkflowTask().execute(running)
        assert second.status == WorkflowStatus.RUNNING
        assert second.context["_sub_workflow_id"] == child_id, "a redelivery started a second child"
        assert store.count_by_application("child-app") == 1
        assert queue.has_pending_message_for_execution(child_id)
    finally:
        queue.close()
        store.close()


def test_a_sub_workflow_in_a_loop_starts_a_new_child_per_generation(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        parent = Workflow.create(
            application="parent-app",
            name="loop",
            stages=[
                _stage("sub", "SubWorkflowTask", context={"_sub_workflow_config": _child_config()}),
                _stage("back", "jump_once_sub", requisite_stage_ref_ids={"sub"}, context={"jump_target": "sub"}),
            ],
        )
        Orchestrator(queue, store=store).start(parent)
        processor.process_all(timeout=40)
        done = store.retrieve(parent.id)
        assert done.status == WorkflowStatus.SUCCEEDED, done.status
        assert store.count_by_application("child-app") == 2, "the second pass reused the first child"
        assert _Record.runs == ["c0", "c0"]
    finally:
        queue.close()
        store.close()


def test_a_sub_workflow_timeout_comes_from_the_context_and_cancels_the_child(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        config = _child_config(task_type="TimerTask", context={"duration_seconds": 3600})
        parent = Workflow.create(
            application="parent-app",
            name="timeout",
            stages=[
                _stage(
                    "sub",
                    "SubWorkflowTask",
                    context={"_sub_workflow_config": config, "sub_workflow_timeout_seconds": 1},
                )
            ],
        )
        Orchestrator(queue, store=store).start(parent)
        processor.process_all(timeout=30)
        done = store.retrieve(parent.id)
        assert done.status == WorkflowStatus.TERMINAL, done.status
        child_id = done.stages[0].context.get("_sub_workflow_id")
        assert child_id
        child = store.retrieve(child_id)
        assert child.is_canceled or child.status == WorkflowStatus.CANCELED, child.status
    finally:
        queue.close()
        store.close()


class _RaisesTransient(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        raise TransientError("try again later")


class _UnpicklableError(Exception):
    def __init__(self) -> None:
        super().__init__("cannot cross the process boundary")
        self.handle = lambda: None


class _RaisesUnpicklable(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        raise _UnpicklableError()


class _Sleeps(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        time.sleep(5)
        return TaskResult.success()

    def on_timeout(self, stage: StageExecution) -> TaskResult | None:
        return TaskResult.success(outputs={"timed_out": True})


def _bare_stage() -> StageExecution:
    return StageExecution(ref_id="iso", type="x", name="ISO", tasks=[_task("x")], context={"k": 1})


def test_process_isolation_returns_the_tasks_own_exception() -> None:
    from stabilize.resilience.process_executor import ProcessIsolatedTaskExecutor

    executor = ProcessIsolatedTaskExecutor(timeout_seconds=30)
    with pytest.raises(TransientError, match="try again later"):
        executor.execute(_RaisesTransient(), _bare_stage())
    with pytest.raises(RuntimeError, match="_UnpicklableError.*cannot cross the process boundary"):
        executor.execute(_RaisesUnpicklable(), _bare_stage())


def test_process_isolation_timeout_runs_on_timeout() -> None:
    from stabilize.resilience.process_executor import ProcessIsolatedTaskExecutor

    executor = ProcessIsolatedTaskExecutor(timeout_seconds=30)
    result = executor.execute(_Sleeps(), _bare_stage(), timeout_seconds=0.5)
    assert result.status == WorkflowStatus.SUCCEEDED
    assert result.outputs == {"timed_out": True}


def test_sub_workflow_is_refused_under_process_isolation() -> None:
    from stabilize.resilience.process_executor import ProcessIsolatedTaskExecutor
    from stabilize.tasks.sub_workflow import SubWorkflowTask

    stage = _bare_stage()
    stage.context["_sub_workflow_config"] = _child_config()
    result = ProcessIsolatedTaskExecutor(timeout_seconds=30).execute(SubWorkflowTask(), stage)
    assert result.status == WorkflowStatus.TERMINAL
    assert "process isolation" in str(result.context.get("exception", result.context)).lower()
