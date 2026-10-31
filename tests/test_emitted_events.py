"""The documented events the engine never emitted (A20, B42), the stream's tenant, and the docs pins (A18, A19, B43)."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.events import (
    EventType,
    configure_event_sourcing,
    get_event_bus,
    reset_event_bus,
    reset_event_recorder,
)
from stabilize.events.base import CURRENT_SCHEMA_VERSION, EntityType, Event, get_event_migrator
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult
from tests.conftest import setup_stabilize

DOCS = Path(__file__).resolve().parents[1] / "docs" / "guide"


class RevealingTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"secret": "hunter2", "n": 1}, context={"flag": True})


@pytest.fixture
def stack(
    backend: str, tmp_path: Path, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Any]:
    reset_event_bus()
    reset_event_recorder()
    audit_key = getattr(request, "param", None)
    if audit_key is not None:
        monkeypatch.setenv("STABILIZE_AUDIT_KEY", audit_key)
    else:
        monkeypatch.delenv("STABILIZE_AUDIT_KEY", raising=False)
    if backend == "sqlite":
        from stabilize.events.store.sqlite.store import SqliteEventStore
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        url = f"sqlite:///{tmp_path / 'emit.db'}"
        store: Any = SqliteWorkflowStore(url, create_tables=True)
        queue: Any = SqliteQueue(url)
        queue._create_table()
        event_store: Any = SqliteEventStore(url, create_tables=True)
    else:
        pytest.importorskip("psycopg")
        from stabilize.events.store.postgres.store import PostgresEventStore
        from stabilize.persistence.postgres import PostgresWorkflowStore
        from stabilize.queue.postgres import PostgresQueue

        url = request.getfixturevalue("postgres_url")
        store = PostgresWorkflowStore(url)
        queue = PostgresQueue(url, table_name="queue_messages")
        event_store = PostgresEventStore(url)
    recorder = configure_event_sourcing(event_store)
    try:
        yield store, queue, event_store, recorder
    finally:
        for resource in (queue, store, event_store):
            close = getattr(resource, "close", None)
            if callable(close):
                close()
        reset_event_bus()
        reset_event_recorder()


def _workflow(task_type: str = "success") -> Workflow:
    stage = StageExecution(
        ref_id="s1",
        type="work",
        name="Work",
        tasks=[TaskExecution.create(name="t", implementing_class=task_type, stage_start=True, stage_end=True)],
    )
    return Workflow.create(application="emit", name="w", stages=[stage])


def _of(event_store: Any, workflow_id: str, event_type: EventType) -> list[Event]:
    return [e for e in event_store.get_events_for_workflow(workflow_id) if e.event_type == event_type]


def test_store_pause_and_resume_emit_events(stack: Any) -> None:
    store, _queue, event_store, _recorder = stack
    workflow = _workflow()
    workflow.status = WorkflowStatus.RUNNING
    store.store(workflow)

    store.pause(workflow.id, "ops")
    paused = _of(event_store, workflow.id, EventType.WORKFLOW_PAUSED)
    assert len(paused) == 1, "pausing through the store recorded no workflow.paused event"
    assert paused[0].data["paused_by"] == "ops"
    assert isinstance(paused[0].data["pause_time"], int)
    store.pause(workflow.id, "ops")
    assert len(_of(event_store, workflow.id, EventType.WORKFLOW_PAUSED)) == 1, (
        "a pause with no effect recorded an event"
    )

    store.resume(workflow.id)
    resumed = _of(event_store, workflow.id, EventType.WORKFLOW_RESUMED)
    assert len(resumed) == 1, "resuming through the store recorded no workflow.resumed event"
    assert isinstance(resumed[0].data["resume_time"], int)
    assert resumed[0].data["paused_ms"] >= 0
    store.resume(workflow.id)
    assert len(_of(event_store, workflow.id, EventType.WORKFLOW_RESUMED)) == 1, (
        "a resume with no effect recorded an event"
    )
    store.delete(workflow.id)


def test_orchestrator_unpause_emits_resumed(stack: Any) -> None:
    from stabilize.orchestrator import Orchestrator

    store, queue, event_store, _recorder = stack
    workflow = _workflow()
    workflow.status = WorkflowStatus.RUNNING
    workflow.start_time = int(time.time() * 1000)
    store.store(workflow)
    store.pause(workflow.id, "ops")

    Orchestrator(queue, store=store).unpause(workflow)
    assert store.retrieve(workflow.id).status == WorkflowStatus.RUNNING
    assert len(_of(event_store, workflow.id, EventType.WORKFLOW_RESUMED)) == 1, (
        "unpause recorded no workflow.resumed event"
    )
    queue.clear()
    store.delete(workflow.id)


def test_status_changes_are_recorded_for_stages_and_workflows(stack: Any) -> None:
    store, queue, event_store, _recorder = stack
    processor, runner, _registry = setup_stabilize(store, queue)
    workflow = _workflow()
    store.store(workflow)
    runner.start(workflow)
    processor.process_all(timeout=10.0)
    assert store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED

    changes = _of(event_store, workflow.id, EventType.STATUS_CHANGED)
    stage_id = workflow.stages[0].id
    stage_changes = [(e.data["old_status"], e.data["new_status"]) for e in changes if e.entity_id == stage_id]
    assert stage_changes == [("NOT_STARTED", "RUNNING"), ("RUNNING", "SUCCEEDED")], (
        "stage transitions were not recorded"
    )
    workflow_changes = [
        (e.data["old_status"], e.data["new_status"])
        for e in changes
        if e.entity_type == EntityType.WORKFLOW and e.entity_id == workflow.id
    ]
    assert ("RUNNING", "SUCCEEDED") in workflow_changes, "the workflow's completion transition was not recorded"
    assert all(e.entity_type != EntityType.TASK for e in changes), "task transitions are carried by task.* events"
    store.delete(workflow.id)


def test_a_failed_storing_transaction_leaves_no_status_change_behind(
    stack: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.handlers.complete_stage.handler import CompleteStageHandler
    from stabilize.queue.messages import CompleteStage, CompleteWorkflow

    store, queue, event_store, recorder = stack
    workflow = _workflow()
    stage = workflow.stages[0]
    stage.status = WorkflowStatus.RUNNING
    stage.tasks[0].status = WorkflowStatus.SUCCEEDED
    workflow.status = WorkflowStatus.RUNNING
    store.store(workflow)

    from stabilize.persistence.postgres.transaction import PostgresTransaction
    from stabilize.persistence.sqlite.transaction import AtomicTransaction

    for cls in (AtomicTransaction, PostgresTransaction):
        original_push = cls.push_message

        def failing_push(self: Any, msg: Any, delay: float = 0, _original: Any = original_push) -> None:
            if isinstance(msg, CompleteWorkflow):
                raise RuntimeError("injected crash before commit")
            return _original(self, msg, delay)

        monkeypatch.setattr(cls, "push_message", failing_push)

    handler = CompleteStageHandler(queue, store, event_recorder=recorder)
    with pytest.raises(Exception):
        handler.handle(
            CompleteStage(
                execution_type="PIPELINE", execution_id=workflow.id, stage_id=stage.id, message_id="cs-emit-1"
            )
        )
    assert store.retrieve(workflow.id).stages[0].status == WorkflowStatus.RUNNING
    changes = _of(event_store, workflow.id, EventType.STATUS_CHANGED)
    assert not [e for e in changes if e.data["new_status"] == "SUCCEEDED"], (
        "a status.changed event survived a rolled-back transaction"
    )
    queue.clear()
    store.delete(workflow.id)


def _run_revealing(stack: Any) -> tuple[Any, Workflow]:
    store, queue, event_store, _recorder = stack
    processor, runner, _registry = setup_stabilize(store, queue, extra_tasks={"revealing": RevealingTask})
    workflow = _workflow("revealing")
    store.store(workflow)
    runner.start(workflow)
    processor.process_all(timeout=10.0)
    assert store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED
    return event_store, workflow


def test_task_result_changes_are_recorded_by_name_only(stack: Any) -> None:
    event_store, workflow = _run_revealing(stack)
    stage_id = workflow.stages[0].id

    [outputs] = _of(event_store, workflow.id, EventType.OUTPUTS_UPDATED)
    assert outputs.entity_type == EntityType.STAGE and outputs.entity_id == stage_id
    assert outputs.data["changed_keys"] == ["n", "secret"]
    assert "hunter2" not in json.dumps(outputs.data), "an outputs.updated event carried a value"
    assert "digests" not in outputs.data, "digests were recorded without an audit key"

    [context] = _of(event_store, workflow.id, EventType.CONTEXT_UPDATED)
    assert context.entity_id == stage_id and context.data["changed_keys"] == ["flag"]
    assert "True" not in json.dumps(context.data) and "true" not in json.dumps(context.data)
    stack[0].delete(workflow.id)


@pytest.mark.parametrize("stack", ["audit-key-9"], indirect=True)
def test_task_result_changes_carry_digests_under_an_audit_key(stack: Any) -> None:
    from stabilize.definition_hash import canonical_json

    event_store, workflow = _run_revealing(stack)
    [outputs] = _of(event_store, workflow.id, EventType.OUTPUTS_UPDATED)
    assert outputs.data["digest_alg"] == "hmac-sha256/stabilize-canonical-json/1"
    expected = hmac.new(b"audit-key-9", canonical_json("hunter2"), hashlib.sha256).hexdigest()
    assert outputs.data["digests"]["secret"] == expected
    assert outputs.data["digests"]["n"] == hmac.new(b"audit-key-9", canonical_json(1), hashlib.sha256).hexdigest()
    wrong = hmac.new(b"other-key", canonical_json("hunter2"), hashlib.sha256).hexdigest()
    assert outputs.data["digests"]["secret"] != wrong
    assert "hunter2" not in json.dumps(outputs.data)
    stack[0].delete(workflow.id)


def test_schema_version_two_upcasts_version_one_events() -> None:
    assert CURRENT_SCHEMA_VERSION == 2
    migrator = get_event_migrator()
    old = Event(
        event_type=EventType.CONTEXT_UPDATED,
        entity_type=EntityType.STAGE,
        entity_id="s-1",
        workflow_id="w-1",
        data={"context": {"b": 1, "a": 2}},
        schema_version=1,
    )
    migrated = migrator.migrate(old, strict=True)
    assert migrated.schema_version == 2
    assert migrated.data["changed_keys"] == ["a", "b"]
    assert migrated.data["context"] == {"b": 1, "a": 2}

    plain = Event(
        event_type=EventType.STAGE_COMPLETED,
        entity_type=EntityType.STAGE,
        entity_id="s-1",
        workflow_id="w-1",
        data={"status": "SUCCEEDED"},
        schema_version=1,
    )
    same = migrator.migrate(plain, strict=True)
    assert same.schema_version == 2 and same.data == {"status": "SUCCEEDED"}


def test_the_stream_only_passes_its_tenant() -> None:
    from stabilize.streaming import WorkflowStream

    reset_event_bus()
    stream = WorkflowStream("wf-stream-1", tenant="acme")
    got: list[Any] = []
    stream.on_event(got.append)
    bus = get_event_bus()

    def event(tenant: str, marker: str) -> Event:
        return Event(
            event_type=EventType.CUSTOM,
            entity_type=EntityType.WORKFLOW,
            entity_id="wf-stream-1",
            workflow_id="wf-stream-1",
            data={"marker": marker},
            tenant=tenant,
        )

    bus.publish(event("globex", "other"))
    bus.publish(event("acme", "mine"))
    stream.close()
    assert [item.data["marker"] for item in got] == ["mine"], "the stream delivered another tenant's event"


def test_docs_match_the_engine() -> None:
    flow = (DOCS / "flow_control.rst").read_text()
    assert "cancel_remaining=True" not in flow, "the docs still show cancel_remaining=True as working"
    agent = flow.index('ref_id="agent_contact"')
    assert '"deferredChoiceClaim": "on_complete"' in flow[agent : agent + 600], (
        "the agent-versus-timer example would let the timer win the moment it starts"
    )
    sourcing = (DOCS / "event_sourcing.rst").read_text()
    assert "Not yet emitted" not in sourcing
    assert "``status.changed``" in sourcing and "``outputs.updated``" in sourcing
