"""
Repro tests for event/state divergence (audit finding A1-7).

Events were appended to the event store and published to the bus BEFORE the
workflow-state transaction committed. A rolled-back or crashed state write
left phantom events — an audit log claiming state transitions that never
happened — and bus subscribers observed uncommitted state. Recording inside
a store transaction must join that transaction (same-database stores) and
bus publication must be deferred until after commit.
"""

from typing import Any

import pytest

from stabilize.events import SqliteEventStore, get_event_bus, reset_event_bus
from stabilize.events import EventQuery, EventType
from stabilize.events.recorder import EventRecorder
from stabilize.handlers import CompleteStageHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.messages import CompleteStage, CompleteWorkflow
from stabilize.queue.sqlite import SqliteQueue


@pytest.fixture()
def es_env(tmp_path: Any):
    SingletonMeta.reset(ConnectionManager)
    reset_event_bus()
    url = f"sqlite:///{tmp_path}/es_consistency.db"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    queue._create_table()
    event_store = SqliteEventStore(url, create_tables=True)
    recorder = EventRecorder(event_store)
    yield store, queue, event_store, recorder
    reset_event_bus()
    SingletonMeta.reset(ConnectionManager)


def _task(status: WorkflowStatus) -> TaskExecution:
    task = TaskExecution.create(
        name="t", implementing_class="noop", stage_start=True, stage_end=True
    )
    task.status = status
    return task


class TestDeferredBusPublish:
    def test_publish_deferred_until_commit(self, es_env: Any) -> None:
        store, queue, event_store, recorder = es_env
        stage = StageExecution(ref_id="s", name="S", tasks=[_task(WorkflowStatus.SUCCEEDED)])
        execution = Workflow.create(application="t", name="w", stages=[stage])
        store.store(execution)

        seen: list[Any] = []
        get_event_bus().subscribe("test-sub", lambda e: seen.append(e))

        with store.transaction(queue):
            recorder.record_stage_completed(stage, source_handler="test")
            assert seen == [], (
                "event published to the bus before the state transaction committed"
            )
        assert len(seen) == 1, "deferred event was not published after commit"

    def test_rollback_drops_event_and_publish(self, es_env: Any) -> None:
        store, queue, event_store, recorder = es_env
        stage = StageExecution(ref_id="s", name="S", tasks=[_task(WorkflowStatus.SUCCEEDED)])
        execution = Workflow.create(application="t", name="w", stages=[stage])
        store.store(execution)

        seen: list[Any] = []
        get_event_bus().subscribe("test-sub", lambda e: seen.append(e))

        with pytest.raises(RuntimeError, match="injected"):
            with store.transaction(queue):
                recorder.record_stage_completed(stage, source_handler="test")
                raise RuntimeError("injected rollback")

        assert seen == [], "event published although its transaction rolled back"
        stored = list(
            event_store.get_events(EventQuery(event_types={EventType.STAGE_COMPLETED}))
        )
        assert stored == [], (
            "phantom STAGE_COMPLETED persisted although the state transaction rolled back"
        )


class TestCompleteStageNoPhantomEvents:
    def test_failed_completion_txn_leaves_no_phantom_event(
        self, es_env: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If CompleteStage's state transaction fails, no STAGE_COMPLETED
        event may survive in the event store."""
        store, queue, event_store, recorder = es_env

        stage = StageExecution(ref_id="s", name="S", tasks=[_task(WorkflowStatus.SUCCEEDED)])
        stage.status = WorkflowStatus.RUNNING
        execution = Workflow.create(application="t", name="w", stages=[stage])
        execution.status = WorkflowStatus.RUNNING
        store.store(execution)

        handler = CompleteStageHandler(queue, store, event_recorder=recorder)

        from stabilize.persistence.sqlite.transaction import AtomicTransaction

        original_push = AtomicTransaction.push_message

        def failing_push(self: Any, msg: Any, delay: float = 0) -> None:
            if isinstance(msg, CompleteWorkflow):
                raise RuntimeError("injected crash before commit")
            return original_push(self, msg, delay)

        monkeypatch.setattr(AtomicTransaction, "push_message", failing_push)

        # The injected failure propagates (exact type depends on the error
        # path); what matters is that nothing committed.
        with pytest.raises(Exception):
            handler.handle(
                CompleteStage(
                    execution_type="PIPELINE",
                    execution_id=execution.id,
                    stage_id=stage.id,
                    message_id="cs-phantom-1",
                )
            )

        fresh = store.retrieve(execution.id)
        assert fresh.stages[0].status == WorkflowStatus.RUNNING, (
            "stage state committed although the completion transaction failed"
        )

        stored = list(
            event_store.get_events(EventQuery(event_types={EventType.STAGE_COMPLETED}))
        )
        assert stored == [], (
            "phantom STAGE_COMPLETED event recorded although the stage-completion "
            "transaction never committed"
        )
