from __future__ import annotations

from typing import Any

import pytest

from stabilize import StageExecution, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import JumpToStage
from stabilize.tasks.interface import RetryableTask, Task
from tests.audit_support import drive, make_processor, stage, start


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class Hold(RetryableTask):
    def get_timeout(self) -> Any:
        from datetime import timedelta

        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: Any) -> Any:
        from datetime import timedelta

        return timedelta(seconds=30)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")


def _setup(repository: WorkflowStore, queue: Queue) -> tuple[Any, Any, dict[str, StageExecution]]:
    processor = make_processor(repository, queue, {"ok": Ok, "hold": Hold})
    wf = start(
        repository,
        queue,
        [
            stage("a", ["ok"], {"retry_reason": "none"}),
            stage("b1", ["hold"], needs={"a"}),
            stage("b2", ["hold"], needs={"a"}),
        ],
    )
    result = drive(processor, repository, wf.id, timeout=10, idle=1.0)
    by_ref = {s.ref_id: s for s in result.stages}
    assert by_ref["a"].status == WorkflowStatus.SUCCEEDED
    assert by_ref["b1"].status == WorkflowStatus.RUNNING
    assert by_ref["b2"].status == WorkflowStatus.RUNNING
    return processor, wf, by_ref


def _jump(wf: Any, source: StageExecution, ctx: dict[str, Any]) -> JumpToStage:
    return JumpToStage(
        execution_type=wf.type.value,
        execution_id=wf.id,
        stage_id=source.id,
        target_stage_ref_id="a",
        jump_context=ctx,
    )


def test_jump_keeps_a_key_committed_by_a_concurrent_jump(repository: WorkflowStore, queue: Queue) -> None:
    processor, wf, by_ref = _setup(repository, queue)
    handler = processor._handlers[JumpToStage]
    original = handler._apply_jump
    state = {"interleaved": False}

    def interleave(*args: Any, **kwargs: Any) -> None:
        if not state["interleaved"]:
            state["interleaved"] = True
            handler.handle(_jump(wf, by_ref["b2"], {"retry_reason": "b2"}))
        original(*args, **kwargs)

    handler._apply_jump = interleave
    handler.handle(_jump(wf, by_ref["b1"], {"from_b1": True}))
    assert state["interleaved"]
    ctx = repository.retrieve_stage(by_ref["a"].id).context
    assert ctx.get("from_b1") is True
    assert ctx.get("retry_reason") == "b2", f"N5 lost update: retry_reason={ctx.get('retry_reason')!r}"


def test_jump_control_without_interleaving_applies_its_context(repository: WorkflowStore, queue: Queue) -> None:
    processor, wf, by_ref = _setup(repository, queue)
    processor._handlers[JumpToStage].handle(_jump(wf, by_ref["b1"], {"from_b1": True}))
    ctx = repository.retrieve_stage(by_ref["a"].id).context
    assert ctx.get("from_b1") is True
    assert ctx.get("retry_reason") == "none"
    assert ctx.get("_jump_bypass") is True
