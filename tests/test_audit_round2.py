from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

import pytest
from audit_support import drive, make_processor, stage, start

from stabilize import Orchestrator, StageExecution, TaskExecution, TaskResult, Workflow
from stabilize.errors.transient import ConcurrencyError
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.config import reset_handler_config
from stabilize.tasks.interface import RetryableTask, Task

MARKS: dict[str, list[float]] = {}
CALLS = {"cleanup": 0, "exec": 0, "armed": False}


def _mark(key: str) -> None:
    MARKS.setdefault(key, []).append(time.monotonic())


class Main(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        _mark("main")
        return TaskResult.success()


class Fast(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        _mark(f"end:{stage.ref_id}")
        return TaskResult.success()


class Slow(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        first = float(stage.context.get("first", 0) or 0) or time.time()
        if time.time() - first < float(stage.context.get("hold", 1.5)):
            return TaskResult.running(context={"first": first})
        _mark(f"end:{stage.ref_id}")
        return TaskResult.success()


class Fails(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.terminal("before-stage failed")


class Redirect(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult(status=WorkflowStatus.REDIRECT)


class Exits(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        CALLS["exec"] += 1
        raise SystemExit(3)


class Clean(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()

    def on_cleanup(self, stage: StageExecution) -> None:
        CALLS["cleanup"] += 1


TASKS = {"main": Main, "fast": Fast, "slow": Slow, "fails": Fails, "redirect": Redirect, "exits": Exits, "clean": Clean}


def _parent(befores: list[tuple[str, str, dict[str, Any]]], application: str) -> Workflow:
    parent = StageExecution(
        ref_id="parent",
        type="test",
        name="parent",
        tasks=[TaskExecution.create("main", "main", stage_start=True, stage_end=True)],
    )
    stages = [parent]
    for ref, impl, ctx in befores:
        stages.append(
            StageExecution(
                ref_id=ref,
                type="test",
                name=ref,
                context=dict(ctx),
                synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
                tasks=[TaskExecution.create(impl, impl, stage_start=True, stage_end=True)],
            )
        )
    wf = Workflow.create(application=application, name="synthetic", stages=stages)
    for s in wf.stages[1:]:
        s.parent_stage_id = parent.id
    return wf


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    MARKS.clear()
    CALLS.update(cleanup=0, exec=0, armed=False)
    yield
    reset_handler_config()


def _launch(repository: WorkflowStore, queue: Queue, wf: Workflow) -> None:
    repository.store(wf)
    Orchestrator(queue, store=repository).start(wf)


def test_redirect_without_target_ends_its_stage(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(repository, queue, TASKS)
    wf = start(repository, queue, [stage("s", ["fast", "redirect"])])
    result = drive(processor, repository, wf.id, idle=1.5)
    assert result.status.is_complete, f"N1 REDIRECT without target left the workflow {result.status.name}"


def test_recovery_does_not_start_a_parent_before_its_before_stage_ends(repository: WorkflowStore, queue: Queue) -> None:
    app = f"audit-{uuid.uuid4().hex[:8]}"
    processor = make_processor(repository, queue, TASKS, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100)
    processor.config.recovery_application = app
    wf = _parent([("before", "slow", {"hold": 1.5})], app)
    _launch(repository, queue, wf)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        b = next(s for s in repository.retrieve(wf.id).stages if s.ref_id == "before")
        if b.tasks[0].status == WorkflowStatus.RUNNING:
            break
        drive(processor, repository, wf.id, timeout=0.2, idle=0.05)
    processor.run_recovery()
    result = drive(processor, repository, wf.id, timeout=10)
    main_t, end_t = MARKS.get("main", [0.0])[0], MARKS.get("end:before", [1e18])[0]
    assert main_t >= end_t, "N2 the parent's task ran before its before-stage ended"
    assert result.status == WorkflowStatus.SUCCEEDED


def test_failed_continue_before_stage_lets_the_workflow_finish(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(repository, queue, TASKS)
    wf = _parent([("before", "fails", {"continuePipelineOnFailure": True})], "test")
    _launch(repository, queue, wf)
    result = drive(processor, repository, wf.id, idle=2.0)
    assert result.status.is_complete, f"N8 parent wedged after a FAILED_CONTINUE before-stage: {result.status.name}"


def test_parallel_before_stage_is_not_failed_by_the_wait_budget(
    repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STABILIZE_MAX_STAGE_WAIT_RETRIES", "3")
    monkeypatch.setenv("STABILIZE_HANDLER_RETRY_DELAY_S", "0.2")
    reset_handler_config()
    processor = make_processor(repository, queue, TASKS, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100)
    wf = _parent([("b1", "fast", {}), ("b2", "slow", {"hold": 2.0})], "test")
    _launch(repository, queue, wf)
    result = drive(processor, repository, wf.id, timeout=15, idle=2.0)
    b2 = next(s for s in result.stages if s.ref_id == "b2")
    detail = f"B86-2 parent {result.status.name} while before-stage b2 was {b2.status.name}"
    assert result.status == WorkflowStatus.SUCCEEDED, detail


def test_system_exit_from_a_task_is_one_recorded_failure(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(repository, queue, TASKS)
    wf = start(repository, queue, [stage("s", ["exits"])])
    escaped = 0
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not repository.retrieve(wf.id).status.is_complete:
        try:
            if not processor.process_one():
                time.sleep(0.02)
        except Exception:
            pass
        except BaseException:
            escaped += 1
    result = repository.retrieve(wf.id)
    assert escaped == 0, f"N6 SystemExit escaped the processor {escaped} time(s)"
    assert CALLS["exec"] == 1
    assert result.stages[0].tasks[0].status == WorkflowStatus.TERMINAL


class _ConflictOnce:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def store_stage(self, stage_: Any, *args: Any, **kwargs: Any) -> Any:
        if CALLS["armed"] and stage_.status == WorkflowStatus.SUCCEEDED:
            CALLS["armed"] = False
            raise ConcurrencyError("version changed")
        return self._inner.store_stage(stage_, *args, **kwargs)

    def __getattr__(self, item: str) -> Any:
        return getattr(self._inner, item)


def test_on_cleanup_runs_once_across_a_version_conflict(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(repository, queue, TASKS)
    original = repository.transaction

    @contextmanager
    def transaction(q: Any = None) -> Iterator[Any]:
        with original(q) as txn:
            yield _ConflictOnce(txn)

    repository.transaction = transaction  # type: ignore[method-assign]
    CALLS["armed"] = True
    wf = start(repository, queue, [stage("s", ["clean"])])
    result = drive(processor, repository, wf.id)
    assert result.status == WorkflowStatus.SUCCEEDED
    assert CALLS["cleanup"] == 1, f"N4 on_cleanup ran {CALLS['cleanup']} times across one conflict"
