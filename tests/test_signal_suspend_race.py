"""A persistent signal handled while its stage suspends on another worker is delivered, not stranded (#107)."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from stabilize import Orchestrator, QueueProcessor, StageExecution, Task, TaskRegistry, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import RunTask, SignalStage


class SuspendUntilSignalled(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.context.get("_signal_data") is not None:
            return TaskResult.success(outputs={"resumed_with": stage.context["_signal_name"]})
        return TaskResult.suspend(context={"waiting_for": "stage_exit"})


def _run_until_run_task(processor: QueueProcessor, queue: Queue) -> RunTask:
    for _ in range(20):
        message = queue.poll_one()
        assert message is not None, "the workflow produced no RunTask"
        if isinstance(message, RunTask):
            return message
        processor._handlers[type(message)].handle(message)
        queue.ack(message)
    raise AssertionError("no RunTask within 20 messages")


def _two_worker_stores(backend: str, repository: WorkflowStore, queue: Queue, tmp_path: Path) -> tuple[Any, Any]:
    if backend != "sqlite":
        return repository, queue
    from stabilize.persistence.sqlite import SqliteWorkflowStore
    from stabilize.queue.sqlite import SqliteQueue

    url = f"sqlite:///{tmp_path / 'race.db'}"
    store = SqliteWorkflowStore(url, create_tables=True)
    file_queue = SqliteQueue(url)
    file_queue._create_table()
    return store, file_queue


def test_a_signal_racing_the_suspend_is_delivered(
    repository: WorkflowStore, queue: Queue, backend: str, tmp_path: Path
) -> None:
    repository, queue = _two_worker_stores(backend, repository, queue, tmp_path)
    registry = TaskRegistry()
    registry.register("suspender", SuspendUntilSignalled)
    processor = QueueProcessor(queue, store=repository, task_registry=registry)
    wf = Workflow.create(
        application="signal-race",
        name="race",
        stages=[
            StageExecution(
                ref_id="gate",
                name="Advice drafted",
                tasks=[TaskExecution.create("wait", "suspender", stage_start=True, stage_end=True)],
            )
        ],
    )
    repository.store(wf)
    Orchestrator(queue).start(wf)
    run_task = _run_until_run_task(processor, queue)
    stage = repository.retrieve(wf.id).stage_by_ref_id("gate")
    assert stage.status == WorkflowStatus.RUNNING

    status_read = threading.Event()
    suspend_committed = threading.Event()
    original = repository.pending_signal_count

    def paused_count(execution_id: str, stage_ref_id: str) -> int:
        status_read.set()
        assert suspend_committed.wait(30), "the suspend never committed"
        return original(execution_id, stage_ref_id)

    repository.pending_signal_count = paused_count  # type: ignore[method-assign]
    signal_handler = processor._handlers[SignalStage]
    signal = SignalStage(
        execution_type=wf.type.value,
        execution_id=wf.id,
        stage_id=stage.id,
        signal_name="stage_exit",
        signal_data={"by": "reviewer"},
        persistent=True,
    )
    failures: list[BaseException] = []

    def signal_worker() -> None:
        try:
            signal_handler.handle(signal)
        except BaseException as exc:
            failures.append(exc)

    worker = threading.Thread(target=signal_worker)
    worker.start()
    try:
        assert status_read.wait(30), "the signal handler never reached its buffer decision"
        processor._handlers[RunTask].handle(run_task)
        queue.ack(run_task)
        assert repository.retrieve(wf.id).stage_by_ref_id("gate").status == WorkflowStatus.SUSPENDED
        suspend_committed.set()
        worker.join(60)
    finally:
        repository.pending_signal_count = original  # type: ignore[method-assign]
        suspend_committed.set()
    assert not worker.is_alive() and failures == [], failures

    processor.process_all(timeout=30.0)
    final: Any = repository.retrieve(wf.id).stage_by_ref_id("gate")
    assert final.status == WorkflowStatus.SUCCEEDED, f"stage left {final.status.name} with the signal stranded"
    assert final.outputs.get("resumed_with") == "stage_exit"
    assert repository.pending_signal_count(wf.id, "gate") == 0, "an unconsumed signal row remained"
    if backend == "sqlite":
        queue.close()
        repository.close()
