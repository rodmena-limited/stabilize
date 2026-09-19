"""Ticket #30: the event log must record what the engine actually did.

Seven recorder methods shipped with replay branches and zero call sites, and
there was no event type for suspension at all — so a human-approval wait was an
unexplained silence between task.started and task.completed, a jump left no
trace, and a retry storm was invisible. Each test pairs the new coverage with a
control asserting the previously-wired lifecycle events still fire.
"""

from __future__ import annotations

import pytest

from stabilize import (
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.errors import TransientError
from stabilize.events import configure_event_sourcing, reset_event_recorder
from stabilize.events.base import EventType
from stabilize.events.store.sqlite import SqliteEventStore
from stabilize.hitl import ApprovalTask, approve


class Flaky(Task):
    attempts = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        Flaky.attempts += 1
        if Flaky.attempts == 1:
            raise TransientError("first attempt fails")
        return TaskResult.success()


class JumpOnce(Task):
    jumped = False

    def execute(self, stage: StageExecution) -> TaskResult:
        if not JumpOnce.jumped:
            JumpOnce.jumped = True
            return TaskResult.jump_to("head")
        return TaskResult.success()


class Plain(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def _stage(ref: str, impl: str, reqs: set[str] | None = None) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type=impl,
        name=ref,
        requisite_stage_ref_ids=reqs or set(),
        tasks=[TaskExecution.create(ref, impl, stage_start=True, stage_end=True)],
    )


@pytest.fixture
def engine(tmp_path):
    Flaky.attempts = 0
    JumpOnce.jumped = False
    url = f"sqlite:///{tmp_path / 'wf.db'}"
    event_store = SqliteEventStore(f"sqlite:///{tmp_path / 'ev.db'}")
    reset_event_recorder()
    configure_event_sourcing(event_store, publish_to_bus=False)

    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url, table_name="queue_messages")
    queue._create_table()
    registry = TaskRegistry()
    registry.register("approval", ApprovalTask)
    registry.register("flaky", Flaky)
    registry.register("jumponce", JumpOnce)
    registry.register("plain", Plain)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    runner = Orchestrator(queue, store=store)
    yield store, queue, processor, runner, event_store
    processor.stop(wait=True)
    reset_event_recorder()


def _types(event_store, workflow_id: str) -> set[str]:
    return {e.event_type.value for e in event_store.get_events_for_workflow(workflow_id)}


def test_lifecycle_events_are_still_recorded(engine) -> None:
    """Control: the previously-wired events must keep firing."""
    store, _q, processor, runner, event_store = engine
    wf = Workflow.create(application="c30", name="plain", stages=[_stage("only", "plain")])
    store.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    types = _types(event_store, wf.id)
    assert {"workflow.created", "workflow.started", "stage.started", "task.started"} <= types


def test_suspension_is_recorded(engine) -> None:
    store, _q, processor, runner, event_store = engine
    wf = Workflow.create(application="c30", name="gate", stages=[_stage("gate", "approval")])
    store.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    assert EventType.STAGE_SUSPENDED.value in _types(event_store, wf.id)


def test_resume_records_the_approver_as_actor(engine) -> None:
    store, queue, processor, runner, event_store = engine
    wf = Workflow.create(application="c30", name="gate", stages=[_stage("gate", "approval")])
    store.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    gate = store.retrieve(wf.id).stages[0]
    approve(queue, execution_id=wf.id, stage_id=gate.id, data={"ok": True}, user="alice")
    processor.process_all(timeout=30.0)

    events = event_store.get_events_for_workflow(wf.id)
    resumed = [e for e in events if e.event_type is EventType.STAGE_RESUMED]
    assert len(resumed) == 1
    assert resumed[0].metadata.actor == "alice"
    assert resumed[0].data["signal_name"] == "approve"


def test_unattributed_events_still_record_as_system(engine) -> None:
    """Control: attribution must not invent an actor where none is known."""
    store, _q, processor, runner, event_store = engine
    wf = Workflow.create(application="c30", name="plain", stages=[_stage("only", "plain")])
    store.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    actors = {e.metadata.actor for e in event_store.get_events_for_workflow(wf.id)}
    assert actors == {"system"}


def test_jump_is_recorded(engine) -> None:
    store, _q, processor, runner, event_store = engine
    wf = Workflow.create(
        application="c30",
        name="loop",
        stages=[_stage("head", "plain"), _stage("looper", "jumponce", {"head"})],
    )
    store.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    events = event_store.get_events_for_workflow(wf.id)
    jumps = [e for e in events if e.event_type is EventType.JUMP_EXECUTED]
    assert len(jumps) == 1
    assert jumps[0].data["jump_type"] in {"backward", "forward", "self_loop"}


def test_task_retry_is_recorded(engine) -> None:
    store, _q, processor, runner, event_store = engine
    wf = Workflow.create(application="c30", name="retry", stages=[_stage("head", "flaky")])
    store.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    retries = [
        e
        for e in event_store.get_events_for_workflow(wf.id)
        if e.event_type is EventType.TASK_RETRIED
    ]
    assert len(retries) >= 1


def test_replay_shows_a_suspended_stage_as_suspended(engine) -> None:
    from stabilize.events.replay import EventReplayer

    store, _q, processor, runner, event_store = engine
    wf = Workflow.create(application="c30", name="gate", stages=[_stage("gate", "approval")])
    store.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    state = EventReplayer(event_store).rebuild_workflow_state(wf.id)
    stages = state["stages"] if isinstance(state, dict) else state.stages
    statuses = {s.get("status") for s in stages.values()}
    assert "SUSPENDED" in statuses
