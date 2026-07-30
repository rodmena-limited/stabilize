"""
Repro tests for non-atomic jumps (audit finding A2-9).

JumpToStageHandler committed each stage mutation in its own transaction
(source status, skip-region stages, target reset, synthetic resets) before
the final transaction that pushes StartStage. A crash partway through leaves
the jump half-applied: e.g. the source already SUCCEEDED but skip-region
stages not yet SKIPPED — which recovery will happily start, contrary to the
jump's semantics. The whole jump must commit atomically with its StartStage.
"""

from typing import Any

import pytest

from stabilize.handlers import JumpToStageHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import JumpToStage, StartStage


def _task(status: WorkflowStatus) -> TaskExecution:
    task = TaskExecution.create(
        name="t", implementing_class="noop", stage_start=True, stage_end=True
    )
    task.status = status
    return task


def _forward_jump_workflow(
    repository: WorkflowStore,
) -> tuple[Workflow, StageExecution, StageExecution, StageExecution]:
    """source(RUNNING) -> middle(NOT_STARTED) -> target(NOT_STARTED)."""
    source = StageExecution(ref_id="source", name="Source", tasks=[_task(WorkflowStatus.RUNNING)])
    source.status = WorkflowStatus.RUNNING
    middle = StageExecution(
        ref_id="middle",
        name="Middle",
        requisite_stage_ref_ids={"source"},
        tasks=[_task(WorkflowStatus.NOT_STARTED)],
    )
    target = StageExecution(
        ref_id="target",
        name="Target",
        requisite_stage_ref_ids={"middle"},
        tasks=[_task(WorkflowStatus.NOT_STARTED)],
    )
    execution = Workflow.create(
        application="test", name="jump atomicity", stages=[source, middle, target]
    )
    execution.status = WorkflowStatus.RUNNING
    repository.store(execution)
    return execution, source, middle, target


class TestJumpAtomicity:
    def test_failed_jump_commit_leaves_no_partial_state(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If the jump's final commit fails (crash surrogate), NO stage may
        have been mutated: the jump either fully applies or not at all."""
        execution, source, middle, target = _forward_jump_workflow(repository)

        handler = JumpToStageHandler(queue, repository)
        message = JumpToStage(
            execution_type="PIPELINE",
            execution_id=execution.id,
            stage_id=source.id,
            target_stage_ref_id="target",
            message_id="jmp-atomic-1",
        )

        # Inject a failure at the StartStage push — the last step of the jump.
        # The transaction class differs per backend, so patch the one in use.
        if backend == "postgres":
            from stabilize.persistence.postgres.transaction import (
                PostgresTransaction as _Txn,
            )
        else:
            from stabilize.persistence.sqlite.transaction import (
                AtomicTransaction as _Txn,
            )

        original_push = _Txn.push_message

        def failing_push(self: Any, msg: Any, delay: float = 0) -> None:
            if isinstance(msg, StartStage):
                raise RuntimeError("injected crash at jump commit")
            return original_push(self, msg, delay)

        monkeypatch.setattr(_Txn, "push_message", failing_push)

        with pytest.raises(RuntimeError, match="injected crash"):
            handler.handle(message)

        fresh = repository.retrieve(execution.id)
        by_ref = {s.ref_id: s for s in fresh.stages}
        assert by_ref["source"].status == WorkflowStatus.RUNNING, (
            f"source committed as {by_ref['source'].status} although the jump never committed"
        )
        assert by_ref["middle"].status == WorkflowStatus.NOT_STARTED
        assert by_ref["target"].status == WorkflowStatus.NOT_STARTED
        assert "_jump_bypass" not in by_ref["target"].context

    def test_forward_jump_applies_fully_and_atomically(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """The happy path still fully applies: source SUCCEEDED, skip region
        SKIPPED, target reset with bypass flag, StartStage pushed, source
        message marked processed."""
        execution, source, middle, target = _forward_jump_workflow(repository)

        handler = JumpToStageHandler(queue, repository)
        message = JumpToStage(
            execution_type="PIPELINE",
            execution_id=execution.id,
            stage_id=source.id,
            target_stage_ref_id="target",
            message_id="jmp-atomic-2",
        )
        handler.handle(message)

        fresh = repository.retrieve(execution.id)
        by_ref = {s.ref_id: s for s in fresh.stages}
        assert by_ref["source"].status == WorkflowStatus.SUCCEEDED
        assert by_ref["middle"].status == WorkflowStatus.SKIPPED
        assert by_ref["target"].status == WorkflowStatus.NOT_STARTED
        assert by_ref["target"].context.get("_jump_bypass") is True
        assert by_ref["target"].context.get("_jump_count") == 1
        assert repository.is_message_processed("jmp-atomic-2")

        start_messages = []
        while True:
            m = queue.poll_one()
            if m is None:
                break
            if isinstance(m, StartStage):
                start_messages.append(m)
            queue.ack(m)
        assert [m.stage_id for m in start_messages] == [target.id]

    def test_backward_jump_resets_source_atomically(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """Backward (retry-loop) jump: source and target reset, jump count
        recorded, StartStage pushed."""
        target = StageExecution(ref_id="target", name="Target", tasks=[_task(WorkflowStatus.SUCCEEDED)])
        target.status = WorkflowStatus.SUCCEEDED
        source = StageExecution(
            ref_id="source",
            name="Source",
            requisite_stage_ref_ids={"target"},
            tasks=[_task(WorkflowStatus.RUNNING)],
        )
        source.status = WorkflowStatus.RUNNING
        execution = Workflow.create(
            application="test", name="backward jump", stages=[target, source]
        )
        execution.status = WorkflowStatus.RUNNING
        repository.store(execution)

        handler = JumpToStageHandler(queue, repository)
        handler.handle(
            JumpToStage(
                execution_type="PIPELINE",
                execution_id=execution.id,
                stage_id=source.id,
                target_stage_ref_id="target",
                message_id="jmp-atomic-3",
            )
        )

        fresh = repository.retrieve(execution.id)
        by_ref = {s.ref_id: s for s in fresh.stages}
        assert by_ref["source"].status == WorkflowStatus.NOT_STARTED
        assert by_ref["target"].status == WorkflowStatus.NOT_STARTED
        assert by_ref["target"].context.get("_jump_count") == 1
