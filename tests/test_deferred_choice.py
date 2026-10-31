"""WI-4 slice 4c: deferred choice settlement, blocking failure, and cancellation of live branches."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest

from stabilize.models.stage import StageExecution, SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import ContinueParentStage
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "mgchoice") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


def _parts(url: str) -> tuple[Any, Any]:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    return PostgresWorkflowStore(url), PostgresQueue(url)


class _Record(Task):
    runs: list[str] = []
    cancelled: list[str] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        _Record.runs.append(stage.ref_id)
        return TaskResult.success()

    def on_cancel(self, stage: StageExecution) -> TaskResult | None:
        _Record.cancelled.append(stage.ref_id)
        return None


class _Slow(_Record):
    seconds: dict[str, list[float]] = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        plan = _Slow.seconds.get(stage.ref_id, [2.0])
        delay = plan.pop(0) if len(plan) > 1 else plan[0]
        time.sleep(delay)
        _Record.runs.append(stage.ref_id)
        return TaskResult.success(outputs={"member": stage.ref_id})


class _Suspend(_Record):
    def execute(self, stage: StageExecution) -> TaskResult:
        _Record.runs.append(stage.ref_id)
        return TaskResult(status=WorkflowStatus.SUSPENDED)


class _Fail(_Record):
    def execute(self, stage: StageExecution) -> TaskResult:
        _Record.runs.append(stage.ref_id)
        delay = stage.context.get("fail_after", 0)
        if delay:
            time.sleep(delay)
        return TaskResult.terminal(f"{stage.ref_id} failed")


class _FailContinue(_Record):
    def execute(self, stage: StageExecution) -> TaskResult:
        _Record.runs.append(stage.ref_id)
        return TaskResult.failed_continue("soft failure")


class _JumpOnce(_Record):
    jumped: list[str] = []
    winners: list[Any] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        _JumpOnce.winners.append(stage.context.get("member"))
        if not _JumpOnce.jumped:
            _JumpOnce.jumped.append(stage.ref_id)
            return TaskResult.jump_to(stage.context["jump_target"])
        return TaskResult.success()


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
    registry.register("record_dc", _Record)
    registry.register("slow_dc", _Slow)
    registry.register("suspend_dc", _Suspend)
    registry.register("fail_dc", _Fail)
    registry.register("fail_continue_dc", _FailContinue)
    registry.register("jump_once_dc", _JumpOnce)
    return QueueProcessor(
        queue,
        store=store,
        task_registry=registry,
        config=QueueProcessorConfig(**config),
        handler_config=HandlerConfig(handler_retry_delay_seconds=0.1),
    )


def _reset() -> None:
    _Record.runs.clear()
    _Record.cancelled.clear()
    _Slow.seconds.clear()
    _JumpOnce.jumped.clear()
    _JumpOnce.winners.clear()


def _run_threaded(processor: Any, store: Any, queue: Any, wf: Workflow, seconds: float = 30) -> Workflow:
    processor.start()
    deadline = time.monotonic() + seconds
    try:
        while time.monotonic() < deadline:
            current = store.retrieve(wf.id)
            if current.status.is_complete and queue.next_ready_in() is None and processor.active_count == 0:
                time.sleep(0.3)
                return store.retrieve(wf.id)
            time.sleep(0.1)
        return store.retrieve(wf.id)
    finally:
        processor.stop()


def test_on_start_losers_are_skipped_and_the_workflow_succeeds(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="dc",
            name="on-start",
            stages=[
                _stage("a", "record_dc", deferred_choice_group="g"),
                _stage("b", "record_dc", deferred_choice_group="g"),
                _stage("after", "record_dc", requisite_stage_ref_ids={"a", "b"}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        processor.process_all(timeout=20)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.SUCCEEDED, done.status
        statuses = {s.ref_id: s.status for s in done.stages}
        assert {statuses["a"], statuses["b"]} == {WorkflowStatus.SKIPPED, WorkflowStatus.SUCCEEDED}, statuses
        loser = next(s for s in done.stages if s.ref_id in ("a", "b") and s.status == WorkflowStatus.SKIPPED)
        assert loser.context.get("_pruned") is True
        assert statuses["after"] == WorkflowStatus.SUCCEEDED
        assert _Record.runs.count("after") == 1
    finally:
        queue.close()
        store.close()


def test_inconsistent_claim_modes_are_refused() -> None:
    with pytest.raises(ValueError, match="deferredChoiceClaim"):
        Workflow.create(
            application="dc",
            name="bad",
            stages=[
                _stage("a", "record_dc", deferred_choice_group="g", context={"deferredChoiceClaim": "on_complete"}),
                _stage("b", "record_dc", deferred_choice_group="g"),
            ],
        )
    with pytest.raises(ValueError, match="deferredChoiceClaim"):
        Workflow.create(
            application="dc",
            name="bad2",
            stages=[_stage("a", "record_dc", deferred_choice_group="g", context={"deferredChoiceClaim": "later"})],
        )


def test_on_complete_timer_beats_a_suspended_wait(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="dc",
            name="timer-vs-signal",
            stages=[
                _stage(
                    "timer",
                    "TimerTask",
                    deferred_choice_group="g",
                    context={"deferredChoiceClaim": "on_complete", "duration_seconds": 0.5},
                ),
                _stage(
                    "wait_signal",
                    "suspend_dc",
                    deferred_choice_group="g",
                    context={"deferredChoiceClaim": "on_complete"},
                ),
                _stage("after", "record_dc", requisite_stage_ref_ids={"timer", "wait_signal"}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        processor.process_all(timeout=30)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.SUCCEEDED, done.status
        by_ref = {s.ref_id: s for s in done.stages}
        assert by_ref["timer"].status == WorkflowStatus.SUCCEEDED
        assert by_ref["wait_signal"].status == WorkflowStatus.SKIPPED
        assert by_ref["wait_signal"].context.get("_pruned") is True
        assert by_ref["wait_signal"].tasks[0].status == WorkflowStatus.CANCELED
        assert by_ref["after"].status == WorkflowStatus.SUCCEEDED
        assert _Record.runs.count("after") == 1
    finally:
        queue.close()
        store.close()


def test_on_complete_two_terminal_members_end_the_workflow_once(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="dc",
            name="both-fail",
            stages=[
                _stage("f1", "fail_dc", deferred_choice_group="g", context={"deferredChoiceClaim": "on_complete"}),
                _stage(
                    "f2",
                    "fail_dc",
                    deferred_choice_group="g",
                    context={"deferredChoiceClaim": "on_complete", "fail_after": 0.5},
                ),
                _stage("after", "record_dc", requisite_stage_ref_ids={"f1", "f2"}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        processor.process_all(timeout=30)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.TERMINAL, done.status
        by_ref = {s.ref_id: s for s in done.stages}
        first = by_ref["f1"]
        second = by_ref["f2"]
        assert first.status == WorkflowStatus.SKIPPED, first.status
        assert first.context.get("_pruned") is True and first.context.get("_deferred_choice_failed") is True
        assert "f1 failed" in str(first.context), "the pruned member lost its failure reason"
        assert second.status == WorkflowStatus.TERMINAL, second.status
        assert _Record.runs.count("after") == 0
    finally:
        queue.close()
        store.close()


def test_a_loop_over_a_deferred_choice_is_decided_afresh(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    _Slow.seconds["a"] = [0.2, 1.5]
    _Slow.seconds["b"] = [1.5, 0.2]
    store, queue = _parts(db)
    processor = _processor(store, queue, poll_frequency_ms=50, max_workers=4)
    try:
        wf = Workflow.create(
            application="dc",
            name="loop",
            stages=[
                _stage("entry", "record_dc"),
                _stage(
                    "a",
                    "slow_dc",
                    requisite_stage_ref_ids={"entry"},
                    deferred_choice_group="g",
                    context={"deferredChoiceClaim": "on_complete"},
                ),
                _stage(
                    "b",
                    "slow_dc",
                    requisite_stage_ref_ids={"entry"},
                    deferred_choice_group="g",
                    context={"deferredChoiceClaim": "on_complete"},
                ),
                _stage("back", "jump_once_dc", requisite_stage_ref_ids={"a", "b"}, context={"jump_target": "entry"}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        done = _run_threaded(processor, store, queue, wf, seconds=40)
        assert done.status == WorkflowStatus.SUCCEEDED, done.status
        assert _JumpOnce.winners == ["a", "b"], f"round two was not decided by round two's events: {_JumpOnce.winners}"
        by_ref = {s.ref_id: s for s in done.stages}
        assert by_ref["a"].status == WorkflowStatus.SKIPPED and by_ref["a"].outputs == {}, (
            "the pruned member's late result was saved"
        )
    finally:
        queue.close()
        store.close()


def test_blocking_failure_ends_the_workflow_terminal(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="dc",
            name="blocking",
            stages=[
                _stage("soft", "fail_continue_dc", context={"_blocking_failure": True}),
                _stage("after", "record_dc", requisite_stage_ref_ids={"soft"}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        processor.process_all(timeout=15)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.TERMINAL, done.status
        assert next(s for s in done.stages if s.ref_id == "soft").status == WorkflowStatus.TERMINAL
        assert _Record.runs.count("after") == 0
        assert queue.dlq_size() == 0
    finally:
        queue.close()
        store.close()


def test_continue_parent_for_a_completed_parent_is_acknowledged(db: str) -> None:
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(application="dc", name="cps", stages=[_stage("p", "record_dc")])
        store.store(wf)
        stage = store.retrieve_stage(wf.stages[0].id)
        stage.status = WorkflowStatus.SUCCEEDED
        store.store_stage(stage)
        execution = store.retrieve(wf.id)
        execution.status = WorkflowStatus.RUNNING
        store.update_status(execution)
        queue.push(
            ContinueParentStage(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=stage.id,
                phase=SyntheticStageOwner.STAGE_AFTER,
            )
        )
        assert processor.process_one() is True
        assert queue.dlq_size() == 0
        assert queue.size() == 0
        assert store.retrieve_stage(stage.id).status == WorkflowStatus.SUCCEEDED

        halted = store.retrieve_stage(stage.id)
        halted.status = WorkflowStatus.TERMINAL
        store.store_stage(halted)
        queue.push(
            ContinueParentStage(
                execution_type="PIPELINE",
                execution_id=wf.id,
                stage_id=stage.id,
                phase=SyntheticStageOwner.STAGE_BEFORE,
            )
        )
        assert processor.process_one() is True
        assert queue.dlq_size() == 0
        follow_up = queue.poll_one()
        assert type(follow_up).__name__ == "CompleteWorkflow", follow_up
    finally:
        queue.close()
        store.close()


def test_terminal_completion_cancels_a_suspended_branch(db: str) -> None:
    from stabilize.orchestrator import Orchestrator

    _reset()
    store, queue = _parts(db)
    processor = _processor(store, queue)
    try:
        wf = Workflow.create(
            application="dc",
            name="suspended-branch",
            stages=[
                _stage("waits", "suspend_dc"),
                _stage("fails", "fail_dc", context={"fail_after": 0.3}),
            ],
        )
        Orchestrator(queue, store=store).start(wf)
        processor.process_all(timeout=20)
        done = store.retrieve(wf.id)
        assert done.status == WorkflowStatus.TERMINAL, done.status
        waits = next(s for s in done.stages if s.ref_id == "waits")
        assert waits.status == WorkflowStatus.CANCELED, waits.status
        assert waits.tasks[0].status == WorkflowStatus.CANCELED
        assert _Record.cancelled.count("waits") == 1
    finally:
        queue.close()
        store.close()
