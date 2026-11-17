"""Ticket 110: a task handled long after its deadline (engine outage, backlog) still gets its real outcome."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import RunTask
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.tasks.interface import Task, TimerTask, WaitTask
from stabilize.tasks.result import TaskResult
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")

HOUR_MS = 3_600_000
MINUTE_MS = 60_000


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mglate") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


@pytest.fixture(params=["postgres", "sqlite"])
def any_db(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{tmp_path / 'late.db'}"
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


class _ShortGraceTimer(TimerTask):
    grace = timedelta(milliseconds=200)


class _Done(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"child_ran": True})


def _processor(store: Any, queue: Any) -> Any:
    from stabilize.queue.processor import QueueProcessor
    from stabilize.resilience.config import HandlerConfig
    from stabilize.tasks.highway.task import HighwayTask
    from stabilize.tasks.registry import TaskRegistry
    from stabilize.tasks.sub_workflow import SubWorkflowTask

    registry = TaskRegistry()
    registry.register("short_grace_timer", _ShortGraceTimer)
    registry.register("shipped_timer", TimerTask)
    registry.register("shipped_wait", WaitTask)
    registry.register("late_done", _Done)
    registry.register("SubWorkflowTask", SubWorkflowTask)
    registry.register("HighwayTask", HighwayTask)
    return QueueProcessor(
        queue,
        store=store,
        task_registry=registry,
        config=QueueProcessorConfig(),
        handler_config=HandlerConfig(handler_retry_delay_seconds=0.1),
    )


def _one_stage(task_type: str, context: dict[str, Any]) -> Workflow:
    task = TaskExecution.create(name="t", implementing_class=task_type, stage_start=True, stage_end=True)
    stage = StageExecution(ref_id="s0", type=task_type, name="S0", tasks=[task], context=context)
    return Workflow.create(application="late", name="w", stages=[stage])


def _running_since(store: Any, queue: Any, wf: Workflow, task_type: str, started_ago_ms: int) -> None:
    store.store(wf)
    stage = store.retrieve_stage(wf.stages[0].id)
    now_ms = int(time.time() * 1000)
    stage.status = WorkflowStatus.RUNNING
    stage.start_time = now_ms - started_ago_ms
    task = stage.tasks[0]
    task.status = WorkflowStatus.RUNNING
    task.start_time = now_ms - started_ago_ms
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
            task_type=task_type,
        )
    )


def _outcome(store: Any, wf: Workflow) -> tuple[str, Any]:
    done = store.retrieve(wf.id)
    return done.status.name, done.stages[0].context.get("exception")


def test_a_timer_first_handled_well_after_its_due_time_and_grace_fires(any_db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        wf = _one_stage("short_grace_timer", {"duration_seconds": 0.3})
        Orchestrator(queue, store=store).start(wf)
        while True:
            message = queue.poll_one()
            assert message is not None
            processor._handle_message(message)
            queue.ack(message)
            if isinstance(message, RunTask):
                break
        armed = store.retrieve_stage(wf.stages[0].id)
        assert armed.status == WorkflowStatus.RUNNING
        assert armed.context["_timer_due_at"] - armed.context["_timer_armed_at"] == 300
        time.sleep(1.5)
        processor.process_all(timeout=30)
        assert _outcome(store, wf) == ("SUCCEEDED", None), "a timer handled 1.2 s after due with a 0.2 s grace failed"
    finally:
        queue.close()
        store.close()


def _armed(armed_ago_ms: int, due_in_ms: int) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    return {"_timer_armed_at": now_ms - armed_ago_ms, "_timer_due_at": now_ms + due_in_ms}


@pytest.mark.parametrize(
    ("case", "started_ago_ms", "armed_ago_ms", "due_in_ms"),
    [
        ("due an hour ago, armed two hours ago", 2 * HOUR_MS, 2 * HOUR_MS, -HOUR_MS),
        ("first run ten minutes late, due now", 11 * MINUTE_MS, MINUTE_MS, 0),
        ("first run ten minutes late, due in one second", 11 * MINUTE_MS, MINUTE_MS, 1000),
        ("never armed, started two hours ago", 2 * HOUR_MS, None, None),
    ],
)
def test_the_shipped_timer_fires_whatever_the_delay(
    any_db: str, case: str, started_ago_ms: int, armed_ago_ms: int | None, due_in_ms: int | None
) -> None:
    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        context: dict[str, Any] = {"duration_seconds": 1}
        if armed_ago_ms is not None and due_in_ms is not None:
            context.update(_armed(armed_ago_ms, due_in_ms))
        wf = _one_stage("shipped_timer", context)
        _running_since(store, queue, wf, "shipped_timer", started_ago_ms)
        processor.process_all(timeout=30)
        assert _outcome(store, wf) == ("SUCCEEDED", None), case
    finally:
        queue.close()
        store.close()


def test_the_shipped_wait_fires_when_its_due_time_passed_two_hours_ago(any_db: str) -> None:
    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        now_s = int(time.time())
        start_s = now_s - 7260
        context = {
            "waitTime": 60,
            "waitStartTime": start_s,
            "_timer_armed_at": start_s * 1000,
            "_timer_due_at": start_s * 1000 + 60_000,
        }
        wf = _one_stage("shipped_wait", context)
        _running_since(store, queue, wf, "shipped_wait", 7260 * 1000)
        processor.process_all(timeout=30)
        assert _outcome(store, wf) == ("SUCCEEDED", None)
        assert store.retrieve(wf.id).stages[0].outputs["waitedSeconds"] >= 7260
    finally:
        queue.close()
        store.close()


def test_a_timer_has_no_lifetime_and_keeps_its_per_call_limit() -> None:
    century = timedelta(days=36500)
    unarmed = StageExecution(ref_id="u", type="timer", name="U", context={"duration_seconds": 60})
    armed = StageExecution(
        ref_id="a",
        type="timer",
        name="A",
        context={"duration_seconds": 60, "_timer_armed_at": 1_000, "_timer_due_at": 61_000},
    )
    for task in (TimerTask(), WaitTask()):
        assert task.get_timeout() >= century
        assert task.get_dynamic_timeout(unarmed) >= century
        assert task.get_dynamic_timeout(armed) >= century
        assert task.get_execution_timeout(unarmed) == timedelta(hours=1)
        assert task.get_execution_timeout(armed) == timedelta(seconds=60) + timedelta(minutes=5)
    assert _ShortGraceTimer().get_execution_timeout(armed) == timedelta(seconds=60, milliseconds=200)


def _sub_parent(timeout_seconds: float, child_task: str, child_context: dict[str, Any]) -> Workflow:
    from stabilize.models.stage.definition import stage_to_definition

    child_task_row = TaskExecution.create(name="t", implementing_class=child_task, stage_start=True, stage_end=True)
    child_stage = StageExecution(ref_id="c0", type=child_task, name="C0", tasks=[child_task_row], context=child_context)
    config = {"application": "late-child", "name": "child", "stages": [stage_to_definition(child_stage)]}
    return _one_stage(
        "SubWorkflowTask", {"_sub_workflow_config": config, "sub_workflow_timeout_seconds": timeout_seconds}
    )


def test_a_child_that_finished_before_the_parent_timeout_was_noticed_gives_its_result(any_db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        parent = _sub_parent(2, "late_done", {})
        Orchestrator(queue, store=store).start(parent)
        started = time.monotonic()
        child_id: str | None = None
        while time.monotonic() - started < 20:
            child_id = store.retrieve_stage(parent.stages[0].id).context.get("_sub_workflow_id")
            if child_id and store.retrieve(child_id).status == WorkflowStatus.SUCCEEDED:
                break
            message = queue.poll_one()
            if message is None:
                time.sleep(0.02)
                continue
            if child_id and isinstance(message, RunTask) and message.execution_id == parent.id:
                queue.reschedule(message, timedelta(seconds=3))
                continue
            processor._handle_message(message)
            queue.ack(message)
        assert child_id and store.retrieve(child_id).status == WorkflowStatus.SUCCEEDED
        assert store.retrieve(parent.id).status == WorkflowStatus.RUNNING, "the parent saw its child before the outage"
        time.sleep(max(0.0, 2.6 - (time.monotonic() - started)))
        processor.process_all(timeout=30)
        done = store.retrieve(parent.id)
        assert done.status == WorkflowStatus.SUCCEEDED, done.stages[0].context.get("exception")
        assert done.stages[0].outputs["sub_workflow_status"] == "SUCCEEDED"
        assert done.stages[0].outputs["sub_workflow_id"] == child_id
    finally:
        queue.close()
        store.close()


def test_a_child_still_running_at_the_parent_timeout_is_cancelled_and_the_parent_fails(any_db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    store, queue = _parts(any_db)
    processor = _processor(store, queue)
    try:
        parent = _sub_parent(1, "shipped_timer", {"duration_seconds": 3600})
        Orchestrator(queue, store=store).start(parent)
        processor.process_all(timeout=30)
        done = store.retrieve(parent.id)
        assert done.status == WorkflowStatus.TERMINAL
        assert "exceeded total timeout" in str(done.stages[0].context.get("exception"))
        child = store.retrieve(done.stages[0].context["_sub_workflow_id"])
        assert child.is_canceled or child.status == WorkflowStatus.CANCELED, child.status
    finally:
        queue.close()
        store.close()


class _Highway(BaseHTTPRequestHandler):
    states: dict[str, dict[str, Any]] = {}
    polls: list[str] = []

    def do_GET(self) -> None:
        run_id = self.path.rsplit("/", 1)[-1]
        _Highway.polls.append(run_id)
        body = json.dumps({"data": _Highway.states[run_id]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


@pytest.fixture()
def highway() -> Iterator[str]:
    _Highway.polls.clear()
    _Highway.states = {
        "run-finished": {"state": "completed", "result": {"answer": 42}},
        "run-failed": {"state": "failed", "error": "step 3 raised"},
        "run-going": {"state": "running", "current_step": "step2"},
    }
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Highway)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _highway_outcome(tmp_path: Path, endpoint: str, run_id: str) -> tuple[str, StageExecution]:
    store, queue = _parts(f"sqlite:///{tmp_path / 'highway.db'}")
    processor = _processor(store, queue)
    try:
        context = {
            "highway_workflow_definition": {"name": "w", "version": "1.0.0", "start_task": "a", "tasks": {}},
            "highway_api_endpoint": endpoint,
            "highway_api_key": "test-key",
            "highway_run_id": run_id,
        }
        wf = _one_stage("HighwayTask", context)
        _running_since(store, queue, wf, "HighwayTask", 2 * HOUR_MS)
        processor.process_all(timeout=30)
        done = store.retrieve(wf.id)
        return done.status.name, done.stages[0]
    finally:
        queue.close()
        store.close()


def test_a_highway_run_that_finished_before_the_timeout_was_noticed_gives_its_result(
    tmp_path: Path, highway: str
) -> None:
    status, stage = _highway_outcome(tmp_path, highway, "run-finished")
    assert _Highway.polls == ["run-finished"]
    assert status == "SUCCEEDED", stage.context.get("exception")
    assert stage.outputs["highway_status"] == "completed"
    assert stage.outputs["highway_result"] == {"answer": 42}


def test_a_highway_run_that_failed_before_the_timeout_was_noticed_reports_its_own_error(
    tmp_path: Path, highway: str
) -> None:
    status, stage = _highway_outcome(tmp_path, highway, "run-failed")
    assert _Highway.polls == ["run-failed"]
    assert status == "TERMINAL"
    assert stage.context.get("error") == "Highway workflow failed: step 3 raised"
    assert stage.context.get("highway_status") == "failed"
    assert "exceeded total timeout" not in str(stage.context.get("exception"))


def test_a_highway_run_still_going_at_the_timeout_fails_with_the_timeout(tmp_path: Path, highway: str) -> None:
    status, stage = _highway_outcome(tmp_path, highway, "run-going")
    assert _Highway.polls == ["run-going"]
    assert status == "TERMINAL"
    assert "exceeded total timeout" in str(stage.context.get("exception"))


def test_an_unreachable_highway_at_the_timeout_fails_with_the_timeout(tmp_path: Path) -> None:
    status, stage = _highway_outcome(tmp_path, "http://127.0.0.1:9", "run-finished")
    assert status == "TERMINAL"
    assert "exceeded total timeout" in str(stage.context.get("exception"))
