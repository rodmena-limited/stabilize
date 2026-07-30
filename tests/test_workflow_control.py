"""Regression tests for workflow-level control messages.

Before CancelWorkflowHandler/RestartStageHandler/ResumeStageHandler/
PauseTaskHandler existed, those messages had no registered handler and the
processor consumed and discarded them: Orchestrator.cancel() returned
success while the workflow kept running, restart/unpause were silent
no-ops, and pausing a workflow lost its in-flight task permanently.
"""

from __future__ import annotations

import threading
import time

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.orchestrator import Orchestrator
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.sqlite import SqliteQueue
from stabilize.tasks.interface import Task, TaskResult
from stabilize.tasks.registry import TaskRegistry
from tests.conftest import FailTask, SuccessTask


class SlowTask(Task):
    """Blocks until released (or ~5s), so a cancel can land mid-run."""

    release = threading.Event()
    started = threading.Event()

    def execute(self, stage: StageExecution) -> TaskResult:
        SlowTask.started.set()
        SlowTask.release.wait(timeout=5.0)
        return TaskResult.success(outputs={"slow": True})


class FlakyTask(Task):
    """Fails terminally on the first call, succeeds afterwards."""

    calls = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        FlakyTask.calls += 1
        if FlakyTask.calls == 1:
            return TaskResult.terminal("first attempt fails")
        return TaskResult.success(outputs={"attempt": FlakyTask.calls})


def _setup(db_url: str = "sqlite:///:memory:") -> tuple[SqliteQueue, SqliteWorkflowStore, QueueProcessor, Orchestrator]:
    queue = SqliteQueue(db_url)
    queue._create_table()
    repository = SqliteWorkflowStore(db_url, create_tables=True)
    registry = TaskRegistry()
    registry.register("success", SuccessTask)
    registry.register("fail", FailTask)
    registry.register("slow", SlowTask)
    registry.register("flaky", FlakyTask)
    processor = QueueProcessor(queue, store=repository, task_registry=registry)
    runner = Orchestrator(queue, store=repository)
    return queue, repository, processor, runner


def _one_stage_workflow(task_class: str, ref_id: str = "1") -> StageExecution:
    return StageExecution(
        ref_id=ref_id,
        type="test",
        name=f"Stage {ref_id}",
        tasks=[
            TaskExecution.create(
                name=f"{task_class} task",
                implementing_class=task_class,
                stage_start=True,
                stage_end=True,
            )
        ],
    )


class TestCancelWorkflow:
    def test_cancel_stops_running_workflow(self, tmp_path) -> None:
        """Orchestrator.cancel() while a stage runs must end in CANCELED."""
        # File-backed DB: the processor's poll thread gets its own SQLite
        # connection, and :memory: databases are per-connection.
        queue, repository, processor, runner = _setup(f"sqlite:///{tmp_path}/control.db")
        SlowTask.release = threading.Event()
        SlowTask.started = threading.Event()

        stage1 = _one_stage_workflow("slow", "1")
        stage2 = _one_stage_workflow("success", "2")
        stage2.requisite_stage_ref_ids = {"1"}
        execution = Workflow.create(application="test", name="Cancelable", stages=[stage1, stage2])
        repository.store(execution)
        runner.start(execution)

        processor.start()
        try:
            assert SlowTask.started.wait(timeout=10.0), "slow task never started"
            runner.cancel(execution, user="tester", reason="unit test cancel")

            deadline = time.monotonic() + 15.0
            result = repository.retrieve(execution.id)
            while not result.status.is_complete and time.monotonic() < deadline:
                time.sleep(0.1)
                result = repository.retrieve(execution.id)
        finally:
            SlowTask.release.set()
            processor.stop()

        assert result.status == WorkflowStatus.CANCELED
        assert result.is_canceled
        assert result.canceled_by == "tester"
        statuses = {s.ref_id: s.status for s in result.stages}
        assert statuses["2"] == WorkflowStatus.CANCELED, "downstream stage must not remain runnable"

    def test_cancel_before_start_cancels_all_stages(self) -> None:
        """Cancel of a NOT_STARTED workflow terminates it, not a silent drop."""
        queue, repository, processor, runner = _setup()
        execution = Workflow.create(
            application="test",
            name="Never runs",
            stages=[_one_stage_workflow("success", "1")],
        )
        repository.store(execution)

        runner.cancel(execution, user="tester", reason="canceled before start")
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.CANCELED
        assert result.is_canceled
        assert all(s.status == WorkflowStatus.CANCELED for s in result.stages)

    def test_cancel_is_idempotent_on_completed_workflow(self) -> None:
        queue, repository, processor, runner = _setup()
        execution = Workflow.create(
            application="test",
            name="Completes",
            stages=[_one_stage_workflow("success", "1")],
        )
        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)
        assert repository.retrieve(execution.id).status == WorkflowStatus.SUCCEEDED

        runner.cancel(execution, user="tester", reason="too late")
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED, "late cancel must not disturb a finished workflow"


class TestRestartStage:
    def test_restart_reruns_terminal_stage_to_success(self) -> None:
        queue, repository, processor, runner = _setup()
        FlakyTask.calls = 0
        execution = Workflow.create(
            application="test",
            name="Restartable",
            stages=[_one_stage_workflow("flaky", "1")],
        )
        repository.store(execution)
        runner.start(execution)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.TERMINAL
        failed_stage = result.stages[0]

        runner.restart(result, failed_stage.id)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.stages[0].status == WorkflowStatus.SUCCEEDED
        assert FlakyTask.calls == 2


class TestPauseResume:
    def test_pause_parks_in_flight_task_and_unpause_completes(self) -> None:
        """PauseTask must park the task (not lose it); ResumeStage re-runs it."""
        queue, repository, processor, runner = _setup()
        execution = Workflow.create(
            application="test",
            name="Pausable",
            stages=[_one_stage_workflow("success", "1")],
        )
        repository.store(execution)
        runner.start(execution)

        # StartWorkflow -> StartStage -> StartTask leaves RunTask queued.
        for _ in range(3):
            assert processor.process_one()

        repository.pause(execution.id, paused_by="tester")

        # RunTask sees PAUSED and converts to PauseTask; PauseTask parks.
        assert processor.process_one()
        assert processor.process_one()

        parked = repository.retrieve(execution.id)
        assert parked.stages[0].status == WorkflowStatus.PAUSED
        assert parked.stages[0].tasks[0].status == WorkflowStatus.PAUSED

        runner.unpause(parked)
        processor.process_all(timeout=10.0)

        result = repository.retrieve(execution.id)
        assert result.status == WorkflowStatus.SUCCEEDED
        assert result.stages[0].status == WorkflowStatus.SUCCEEDED
        assert result.stages[0].tasks[0].status == WorkflowStatus.SUCCEEDED


class TestUnregisteredMessageSafety:
    def test_unregistered_message_is_not_silently_acked(self) -> None:
        """A message with no handler must be retried/DLQd, never ack-dropped."""
        from stabilize.queue.messages import CancelWorkflow

        queue, repository, processor, runner = _setup()
        execution = Workflow.create(
            application="test",
            name="Orphan message",
            stages=[_one_stage_workflow("success", "1")],
        )
        repository.store(execution)

        # Simulate the pre-fix registry hole.
        del processor._handlers[CancelWorkflow]
        runner.cancel(execution, user="tester", reason="dropped?")

        assert queue.size() == 1
        with pytest.raises(RuntimeError, match="No handler registered"):
            processor.process_one()
        assert queue.size() == 1, "unhandled message must remain queued, not be consumed"
