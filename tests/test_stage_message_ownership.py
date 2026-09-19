"""Ticket #25 / audit finding C1: stage-level messages are scoped to a workflow.

``with_stage`` resolves a stage by primary key, so without an ownership check any
caller able to influence ``stage_id`` can drive a stage in a workflow it does not
address. Every test here asserts BOTH directions: a foreign stage id is refused,
and a correctly addressed message still does its job. A one-directional test
would pass if signal delivery were broken outright.
"""

from __future__ import annotations

import pytest

from stabilize import (
    Orchestrator,
    QueueProcessor,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)
from stabilize.hitl import ApprovalTask, approve
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import CancelStage, SkipStage


def _gate_workflow(name: str) -> Workflow:
    stage = StageExecution(
        ref_id="gate",
        type="approval",
        name="Gate",
        tasks=[
            TaskExecution.create(
                name="wait",
                implementing_class="approval",
                stage_start=True,
                stage_end=True,
            )
        ],
    )
    return Workflow.create(application="own", name=name, stages=[stage])


def _gate_then_tail(name: str) -> Workflow:
    """A suspended gate with a NOT_STARTED stage behind it.

    The tail is a legitimate SkipStage target while the gate holds the
    workflow open, so skipping it does not drive the workflow to completion.
    """
    gate = StageExecution(
        ref_id="gate", type="approval", name="Gate",
        tasks=[
            TaskExecution.create(
                name="wait", implementing_class="approval",
                stage_start=True, stage_end=True,
            )
        ],
    )
    tail = StageExecution(
        ref_id="tail", type="approval", name="Tail",
        requisite_stage_ref_ids={"gate"},
        tasks=[
            TaskExecution.create(
                name="wait2", implementing_class="approval",
                stage_start=True, stage_end=True,
            )
        ],
    )
    return Workflow.create(application="own", name=name, stages=[gate, tail])


def _stage_by_ref(store: WorkflowStore, wf_id: str, ref: str) -> StageExecution:
    return next(s for s in store.retrieve(wf_id).stages if s.ref_id == ref)


@pytest.fixture
def engine(repository: WorkflowStore, queue: Queue):
    registry = TaskRegistry()
    registry.register("approval", ApprovalTask)
    processor = QueueProcessor(queue, store=repository, task_registry=registry)
    runner = Orchestrator(queue, store=repository)
    yield repository, queue, processor, runner
    processor.stop(wait=True)


def _gate(store: WorkflowStore, wf_id: str) -> StageExecution:
    return store.retrieve(wf_id).stages[0]


def _two_suspended(engine) -> tuple[Workflow, Workflow]:
    store, _queue, processor, runner = engine
    a, b = _gate_workflow("A"), _gate_workflow("B")
    for wf in (a, b):
        store.store(wf)
        runner.start(wf)
    processor.process_all(timeout=30.0)
    assert _gate(store, a.id).status == WorkflowStatus.SUSPENDED
    assert _gate(store, b.id).status == WorkflowStatus.SUSPENDED
    return a, b


def test_signal_across_workflows_is_refused(engine) -> None:
    store, queue, processor, _runner = engine
    a, b = _two_suspended(engine)

    approve(queue, execution_id=b.id, stage_id=_gate(store, a.id).id, data={"by": "B"})
    processor.process_all(timeout=30.0)

    assert _gate(store, a.id).status == WorkflowStatus.SUSPENDED
    assert _gate(store, a.id).outputs == {}


def test_signal_within_its_own_workflow_still_works(engine) -> None:
    """Control: the refusal above must not be signal delivery being broken."""
    store, queue, processor, _runner = engine
    _a, b = _two_suspended(engine)

    approve(queue, execution_id=b.id, stage_id=_gate(store, b.id).id, data={"by": "B"})
    processor.process_all(timeout=30.0)

    gate = _gate(store, b.id)
    assert gate.status == WorkflowStatus.SUCCEEDED
    assert gate.outputs["approved"] is True


def test_cancel_stage_across_workflows_is_refused(engine) -> None:
    store, queue, processor, _runner = engine
    a, b = _two_suspended(engine)

    queue.push(
        CancelStage(
            execution_type="PIPELINE",
            execution_id=b.id,
            stage_id=_gate(store, a.id).id,
        )
    )
    processor.process_all(timeout=30.0)

    assert _gate(store, a.id).status == WorkflowStatus.SUSPENDED


def test_skip_stage_across_workflows_is_refused(engine) -> None:
    store, queue, processor, runner = engine
    a, b = _gate_then_tail("A"), _gate_then_tail("B")
    for wf in (a, b):
        store.store(wf)
        runner.start(wf)
    processor.process_all(timeout=30.0)

    target = _stage_by_ref(store, a.id, "tail")
    assert target.status == WorkflowStatus.NOT_STARTED

    queue.push(
        SkipStage(execution_type="PIPELINE", execution_id=b.id, stage_id=target.id)
    )
    processor.process_all(timeout=30.0)

    assert _stage_by_ref(store, a.id, "tail").status == WorkflowStatus.NOT_STARTED


def test_skip_stage_within_its_own_workflow_still_works(engine) -> None:
    """Control for the skip refusal."""
    store, queue, processor, runner = engine
    a = _gate_then_tail("A")
    store.store(a)
    runner.start(a)
    processor.process_all(timeout=30.0)

    target = _stage_by_ref(store, a.id, "tail")
    assert target.status == WorkflowStatus.NOT_STARTED

    queue.push(
        SkipStage(execution_type="PIPELINE", execution_id=a.id, stage_id=target.id)
    )
    processor.process_all(timeout=3.0)

    assert _stage_by_ref(store, a.id, "tail").status == WorkflowStatus.SKIPPED


def test_send_signal_with_store_raises_on_foreign_stage(engine) -> None:
    store, queue, _processor, _runner = engine
    a, b = _two_suspended(engine)

    with pytest.raises(ValueError, match="refusing to signal across workflows"):
        approve(queue, execution_id=b.id, stage_id=_gate(store, a.id).id, store=store)


def test_send_signal_with_store_allows_own_stage(engine) -> None:
    """Control: eager validation must not reject correctly addressed signals."""
    store, queue, processor, _runner = engine
    _a, b = _two_suspended(engine)

    approve(queue, execution_id=b.id, stage_id=_gate(store, b.id).id, store=store)
    processor.process_all(timeout=30.0)

    assert _gate(store, b.id).status == WorkflowStatus.SUCCEEDED
