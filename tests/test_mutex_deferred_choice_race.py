"""
Repro tests for mutex / deferred-choice TOCTOU races (audit findings A1-4/5).

_is_mutex_blocked and _is_deferred_choice_claimed are read-then-check fast
paths; the per-row CAS (store_stage expected_phase=NOT_STARTED) only guards
each stage's OWN row, so two DIFFERENT sibling stages racing past the read
both claim successfully — violating WCP-17/39/40 mutual exclusion and WCP-16
exclusive choice. These tests force the racing interleaving by making the
fast-path checks pass for both siblings (as they do when both reads happen
before either write) and require the claim transaction itself to serialize.
"""

from typing import Any

import pytest

from stabilize.handlers import StartStageHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import CancelStage, StartStage


def _task() -> TaskExecution:
    return TaskExecution.create(
        name="t", implementing_class="noop", stage_start=True, stage_end=True
    )


def _start(handler: StartStageHandler, execution: Workflow, stage: StageExecution, mid: str) -> None:
    handler.handle(
        StartStage(
            execution_type="PIPELINE",
            execution_id=execution.id,
            stage_id=stage.id,
            message_id=mid,
        )
    )


class TestMutexRace:
    def test_racing_siblings_cannot_both_hold_mutex(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stage_a = StageExecution(ref_id="a", name="A", mutex_key="shared_db", tasks=[_task()])
        stage_b = StageExecution(ref_id="b", name="B", mutex_key="shared_db", tasks=[_task()])
        execution = Workflow.create(application="test", name="mutex race", stages=[stage_a, stage_b])
        execution.status = WorkflowStatus.RUNNING
        repository.store(execution)

        # Simulate the race: both siblings' reads happen before either write.
        monkeypatch.setattr(StartStageHandler, "_is_mutex_blocked", lambda self, s: False)

        handler = StartStageHandler(queue, repository)
        _start(handler, execution, stage_a, "ss-mutex-a")
        _start(handler, execution, stage_b, "ss-mutex-b")

        fresh = repository.retrieve(execution.id)
        running = [s.ref_id for s in fresh.stages if s.status == WorkflowStatus.RUNNING]
        assert len(running) == 1, (
            f"mutex 'shared_db' held by {running} simultaneously — WCP-39 violated"
        )

    def test_mutex_transfers_after_owner_completes(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stage_a = StageExecution(ref_id="a", name="A", mutex_key="shared_db", tasks=[_task()])
        stage_b = StageExecution(ref_id="b", name="B", mutex_key="shared_db", tasks=[_task()])
        execution = Workflow.create(application="test", name="mutex transfer", stages=[stage_a, stage_b])
        execution.status = WorkflowStatus.RUNNING
        repository.store(execution)

        monkeypatch.setattr(StartStageHandler, "_is_mutex_blocked", lambda self, s: False)
        handler = StartStageHandler(queue, repository)

        _start(handler, execution, stage_a, "ss-xfer-a")

        # Owner finishes; its claim must be transferable.
        fresh = repository.retrieve(execution.id)
        a_fresh = next(s for s in fresh.stages if s.ref_id == "a")
        a_fresh.status = WorkflowStatus.SUCCEEDED
        repository.store_stage(a_fresh)

        _start(handler, execution, stage_b, "ss-xfer-b")

        fresh = repository.retrieve(execution.id)
        b_fresh = next(s for s in fresh.stages if s.ref_id == "b")
        assert b_fresh.status == WorkflowStatus.RUNNING, (
            "mutex was not released/transferred after the owner completed"
        )


class TestDeferredChoiceRace:
    def test_racing_siblings_cannot_both_win_deferred_choice(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        stage_a = StageExecution(ref_id="a", name="A", deferred_choice_group="response", tasks=[_task()])
        stage_b = StageExecution(ref_id="b", name="B", deferred_choice_group="response", tasks=[_task()])
        execution = Workflow.create(application="test", name="choice race", stages=[stage_a, stage_b])
        execution.status = WorkflowStatus.RUNNING
        repository.store(execution)

        monkeypatch.setattr(
            StartStageHandler, "_is_deferred_choice_claimed", lambda self, s: False
        )

        handler = StartStageHandler(queue, repository)
        _start(handler, execution, stage_a, "ss-choice-a")
        _start(handler, execution, stage_b, "ss-choice-b")

        fresh = repository.retrieve(execution.id)
        running = [s.ref_id for s in fresh.stages if s.status == WorkflowStatus.RUNNING]
        assert len(running) == 1, (
            f"deferred choice group 'response' started {running} — WCP-16 violated"
        )

        # The loser must be cancelled (immediately or via a queued CancelStage).
        loser_ref = "b" if running == ["a"] else "a"
        loser = next(s for s in fresh.stages if s.ref_id == loser_ref)
        cancel_msgs = []
        while True:
            m = queue.poll_one()
            if m is None:
                break
            if isinstance(m, CancelStage):
                cancel_msgs.append(m)
            queue.ack(m)
        assert loser.status != WorkflowStatus.RUNNING
        assert cancel_msgs or loser.status == WorkflowStatus.CANCELED, (
            "losing deferred-choice branch was neither cancelled nor queued for cancellation"
        )

    def test_deferred_choice_never_transfers_after_winner_completes(
        self,
        repository: WorkflowStore,
        queue: Queue,
        backend: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Unlike a mutex, an exclusive choice is decided once: a sibling must
        not start even after the winner finished."""
        stage_a = StageExecution(ref_id="a", name="A", deferred_choice_group="grp", tasks=[_task()])
        stage_b = StageExecution(ref_id="b", name="B", deferred_choice_group="grp", tasks=[_task()])
        execution = Workflow.create(application="test", name="choice final", stages=[stage_a, stage_b])
        execution.status = WorkflowStatus.RUNNING
        repository.store(execution)

        monkeypatch.setattr(
            StartStageHandler, "_is_deferred_choice_claimed", lambda self, s: False
        )
        handler = StartStageHandler(queue, repository)

        _start(handler, execution, stage_a, "ss-final-a")

        fresh = repository.retrieve(execution.id)
        a_fresh = next(s for s in fresh.stages if s.ref_id == "a")
        a_fresh.status = WorkflowStatus.SUCCEEDED
        repository.store_stage(a_fresh)

        _start(handler, execution, stage_b, "ss-final-b")

        fresh = repository.retrieve(execution.id)
        b_fresh = next(s for s in fresh.stages if s.ref_id == "b")
        assert b_fresh.status != WorkflowStatus.RUNNING, (
            "deferred-choice loser started after the winner completed"
        )
