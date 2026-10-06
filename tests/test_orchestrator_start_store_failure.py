from __future__ import annotations

import pytest

from stabilize import Orchestrator, StageExecution, TaskExecution, Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue


def _wf() -> Workflow:
    return Workflow.create(
        application="test",
        name="start",
        stages=[
            StageExecution(
                ref_id="s",
                type="test",
                name="s",
                tasks=[TaskExecution.create("t", "success", stage_start=True, stage_end=True)],
            )
        ],
    )


def test_start_raises_and_queues_nothing_when_store_fails(
    repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(execution: Workflow) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr(repository, "store", broken)
    with pytest.raises(RuntimeError, match="disk full"):
        Orchestrator(queue, store=repository).start(_wf())
    assert queue.size() == 0


def test_start_accepts_a_prestored_workflow(repository: WorkflowStore, queue: Queue) -> None:
    wf = _wf()
    repository.store(wf)
    Orchestrator(queue, store=repository).start(wf)
    assert queue.size() == 1


def test_start_tolerates_a_concurrent_store_of_the_same_workflow(
    repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    wf = _wf()
    real_store = repository.store

    def racing(execution: Workflow) -> None:
        real_store(execution)
        raise RuntimeError("duplicate key value violates unique constraint")

    monkeypatch.setattr(repository, "store", racing)
    Orchestrator(queue, store=repository).start(wf)
    assert queue.size() == 1


def test_start_raises_when_a_stage_insert_fails_and_leaves_no_partial_workflow(
    repository: WorkflowStore, queue: Queue
) -> None:
    wf = _wf()
    duplicate = StageExecution(
        ref_id="s2",
        type="test",
        name="s2",
        tasks=[TaskExecution.create("t2", "success", stage_start=True, stage_end=True)],
    )
    duplicate.id = wf.stages[0].id
    wf.stages.append(duplicate)
    with pytest.raises(Exception):
        Orchestrator(queue, store=repository).start(wf)
    assert not repository.exists(wf.id)
    assert queue.size() == 0
