"""
Tests closing the coverage weak-spots identified by the audit baseline.

The existing suite was strong but had thin coverage in a few load-bearing
areas: crash-consistency between the store write and the queue push, DLQ
behavior against the REAL dlq.py (not reimplemented SQL), event-sourced
replay equivalence against a live run, graceful processor stop with an
in-flight task, and poison-message escalation to the DLQ through the live
processor. These tests exercise the real code paths.
"""

import threading
import time
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.connection import ConnectionManager, SingletonMeta
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.queue.sqlite import SqliteQueue
from stabilize.tasks.interface import Task
from stabilize.tasks.result import TaskResult


def _sqlite_env(tmp_path: Any, name: str):
    SingletonMeta.reset(ConnectionManager)
    url = f"sqlite:///{tmp_path}/{name}.db"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    queue._create_table()
    return store, queue, url


class TestCrashConsistencyStoreVsQueue:
    def test_state_and_message_are_all_or_nothing(self, tmp_path: Any, monkeypatch: Any) -> None:
        """A crash between the stage write and the queue push must leave
        NEITHER committed — the atomic transaction is the whole point."""
        store, queue, _ = _sqlite_env(tmp_path, "crash")
        try:
            stage = StageExecution(ref_id="s", name="S", tasks=[])
            stage.status = WorkflowStatus.RUNNING
            execution = Workflow.create(application="t", name="w", stages=[stage])
            store.store(execution)

            from stabilize.persistence.sqlite.transaction import AtomicTransaction

            original_push = AtomicTransaction.push_message

            def crashing_push(self: Any, msg: Any, delay: float = 0) -> None:
                raise RuntimeError("crash between store write and queue push")

            monkeypatch.setattr(AtomicTransaction, "push_message", crashing_push)

            stage.outputs = {"written": "should-not-persist"}
            with pytest.raises(RuntimeError):
                with store.transaction(queue) as txn:
                    txn.store_stage(stage)
                    txn.push_message(StartWorkflow(execution_type="PIPELINE", execution_id=execution.id))

            # Neither side committed.
            assert queue.size() == 0, "message committed although the transaction crashed"
            fresh = store.retrieve(execution.id)
            assert fresh.stages[0].outputs.get("written") != "should-not-persist", (
                "stage write committed although the transaction crashed"
            )
        finally:
            SingletonMeta.reset(ConnectionManager)


class TestRealDLQPaths:
    def test_expired_message_moves_to_dlq_via_real_code(self, tmp_path: Any) -> None:
        """Exercise the actual move_to_dlq/check_and_move_expired, not inline
        SQL: a message past max_attempts is moved to the DLQ and replayable."""
        from datetime import timedelta

        SingletonMeta.reset(ConnectionManager)
        try:
            queue = SqliteQueue(
                f"sqlite:///{tmp_path}/dlq.db",
                lock_duration=timedelta(seconds=0.2),
                max_attempts=1,
            )
            queue._create_table()

            queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="e1"))
            # Claim and reschedule past max_attempts so it becomes DLQ-eligible.
            message = queue.poll_one()
            assert message is not None
            queue.reschedule(message, timedelta(seconds=0))
            time.sleep(0.3)

            moved = queue.check_and_move_expired()
            assert moved >= 1, "expired over-attempted message was not moved to the DLQ"
            assert queue.dlq_size() >= 1

            entries = queue.list_dlq()
            assert entries, "DLQ listing empty after a move"
            replayed = queue.replay_dlq(entries[0]["id"])
            assert replayed is True
            assert queue.size() >= 1, "replayed DLQ message not returned to the main queue"
        finally:
            SingletonMeta.reset(ConnectionManager)


class TestReplayEquivalence:
    def test_live_run_matches_event_sourced_replay(self, tmp_path: Any) -> None:
        """Run a real workflow through the processor, then assert the
        event-sourced rebuild equals the live store's terminal status."""
        from stabilize.events import (
            SqliteEventStore,
            configure_event_sourcing,
            reset_event_bus,
        )
        from stabilize.events.replay import EventReplayer
        from tests.conftest import setup_stabilize

        SingletonMeta.reset(ConnectionManager)
        reset_event_bus()
        url = f"sqlite:///{tmp_path}/replay_eq.db"
        try:
            store = SqliteWorkflowStore(url, create_tables=True)
            queue = SqliteQueue(url)
            queue._create_table()
            event_store = SqliteEventStore(url, create_tables=True)
            configure_event_sourcing(event_store)

            processor, runner, _ = setup_stabilize(store, queue)
            stage = StageExecution(
                ref_id="s",
                type="success",
                name="S",
                tasks=[TaskExecution.create(name="t", implementing_class="success", stage_start=True, stage_end=True)],
            )
            execution = Workflow.create(application="t", name="w", stages=[stage])
            store.store(execution)
            runner.start(execution)
            processor.process_all(timeout=15.0)

            live = store.retrieve(execution.id)
            assert live.status == WorkflowStatus.SUCCEEDED

            rebuilt = EventReplayer(event_store).rebuild_workflow_state(execution.id)
            assert str(rebuilt.get("status")) in {live.status.name, str(live.status)}, (
                f"replayed status {rebuilt.get('status')} != live {live.status.name}"
            )
        finally:
            reset_event_bus()
            SingletonMeta.reset(ConnectionManager)


class _SlowTask(Task):
    started = threading.Event()

    def execute(self, stage: StageExecution) -> TaskResult:
        _SlowTask.started.set()
        time.sleep(1.0)
        return TaskResult.success(outputs={"done": True})


class TestGracefulStopWithInFlightTask:
    def test_stop_waits_for_in_flight_task_no_double_exec(self, tmp_path: Any) -> None:
        """stop(wait=True) must let an in-flight task finish exactly once."""
        from stabilize.tasks.registry import TaskRegistry

        SingletonMeta.reset(ConnectionManager)
        _SlowTask.started = threading.Event()
        try:
            store, queue, _ = _sqlite_env(tmp_path, "stop")
            registry = TaskRegistry()
            registry.register("slow", _SlowTask)

            runs: list[float] = []

            class CountingSlow(_SlowTask):
                def execute(self, stage: StageExecution) -> TaskResult:
                    runs.append(time.monotonic())
                    return super().execute(stage)

            registry.register("slow", CountingSlow, strict=False)

            from stabilize import Orchestrator

            processor = QueueProcessor(
                queue,
                config=QueueProcessorConfig(poll_frequency_ms=20),
                store=store,
                task_registry=registry,
            )
            stage = StageExecution(
                ref_id="s",
                type="slow",
                name="S",
                tasks=[TaskExecution.create(name="t", implementing_class="slow", stage_start=True, stage_end=True)],
            )
            execution = Workflow.create(application="t", name="w", stages=[stage])
            store.store(execution)
            Orchestrator(queue, store=store).start(execution)

            processor.start()
            assert _SlowTask.started.wait(5.0), "task never started"
            processor.stop(wait=True)  # drains the in-flight task, then stops polling

            # The in-flight task must have finished exactly once (stop drains
            # it rather than orphaning or double-executing it).
            assert len(runs) == 1, f"task executed {len(runs)} times across graceful stop"

            # Its follow-on completion messages resume cleanly on restart with
            # NO re-execution of the already-done task.
            processor2 = QueueProcessor(queue, store=store, task_registry=registry)
            processor2.process_all(timeout=10.0)
            processor2.stop()
            result = store.retrieve(execution.id)
            assert result.status == WorkflowStatus.SUCCEEDED
            assert len(runs) == 1, f"task re-executed after restart ({len(runs)} total runs)"
        finally:
            SingletonMeta.reset(ConnectionManager)


class _PoisonTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        raise RuntimeError("poison: always fails")


class TestPoisonMessageToDLQ:
    def test_repeated_failure_lands_in_dlq_via_live_processor(self, tmp_path: Any) -> None:
        """A handler that raises every time is retried and finally lands in
        the DLQ through the live processor loop (no infinite reprocessing)."""
        from datetime import timedelta

        SingletonMeta.reset(ConnectionManager)
        try:
            url = f"sqlite:///{tmp_path}/poison.db"
            store = SqliteWorkflowStore(url, create_tables=True)
            queue = SqliteQueue(url, lock_duration=timedelta(seconds=0.15), max_attempts=2)
            queue._create_table()

            # A handler that always raises for StartWorkflow.
            processor = QueueProcessor(
                queue,
                config=QueueProcessorConfig(poll_frequency_ms=15, retry_delay=timedelta(seconds=0)),
                store=store,
            )

            def always_fail(message: Any) -> None:
                raise RuntimeError("poison")

            processor.register_handler_func(StartWorkflow, always_fail)
            queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="e1"))

            processor.start()
            deadline = time.time() + 8
            while time.time() < deadline:
                if queue.dlq_size() >= 1:
                    break
                queue.check_and_move_expired()
                time.sleep(0.1)
            processor.stop()

            assert queue.dlq_size() >= 1, "poison message never escalated to the DLQ"
        finally:
            SingletonMeta.reset(ConnectionManager)
