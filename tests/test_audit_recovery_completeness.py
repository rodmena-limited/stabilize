from __future__ import annotations

import threading
import uuid
from datetime import timedelta

import pytest
from audit_support import drive, drop_first, make_processor, stage, start

from stabilize import QueueProcessor, StageExecution, TaskRegistry, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.resilience.bulkheads import TaskBulkheadManager
from stabilize.resilience.circuits import WorkflowCircuitFactory
from stabilize.resilience.config import BulkheadConfig, ResilienceConfig
from stabilize.tasks.interface import RetryableTask, Task

COUNT = {"n": 0}
THREADS: list[str] = []


class Work(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        COUNT["n"] += 1
        return TaskResult.success(outputs={"done": True})


class Job(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(minutes=10)

    def execute(self, stage: StageExecution) -> TaskResult:
        THREADS.append(threading.current_thread().name)
        return TaskResult.running()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    COUNT["n"] = 0
    THREADS.clear()


@pytest.mark.parametrize("lost", ["CompleteTask", "CompleteStage", "CompleteWorkflow"])
def test_one_sweep_finishes_after_a_lost_message_without_rerunning(
    repository: WorkflowStore, queue: Queue, lost: str
) -> None:
    app = f"audit-{uuid.uuid4().hex[:8]}"
    processor = make_processor(repository, queue, {"work": Work})
    processor.config.recovery_application = app
    wf = start(repository, queue, [stage("s", ["work"])], application=app)
    drive(processor, repository, wf.id, drop=drop_first(lost))
    processor.run_recovery()
    result = drive(processor, repository, wf.id)
    assert result.status == WorkflowStatus.SUCCEEDED, f"C32/C34 lost {lost}: workflow {result.status.name}"
    assert COUNT["n"] == 1, f"C33 lost {lost}: task executed {COUNT['n']} times"


def test_recovered_run_task_keeps_the_task_bulkhead(repository: WorkflowStore, queue: Queue) -> None:
    rc = ResilienceConfig(bulkheads=(("job", BulkheadConfig(max_concurrent=2, max_queue_size=10)),), database_url=None)
    registry = TaskRegistry()
    registry.register("job", Job)
    processor = QueueProcessor(
        queue,
        store=repository,
        task_registry=registry,
        bulkhead_manager=TaskBulkheadManager(rc),
        circuit_factory=WorkflowCircuitFactory(rc),
    )
    app = f"audit-{uuid.uuid4().hex[:8]}"
    processor.config.recovery_application = app
    wf = start(repository, queue, [stage("s", ["job"])], application=app)
    drive(processor, repository, wf.id, idle=0.5)
    assert THREADS and "stabilize_job" in THREADS[0]
    queue.purge_workflow(wf.id)
    processor.run_recovery()
    drive(processor, repository, wf.id, idle=0.5)
    assert len(THREADS) >= 2 and "stabilize_job" in THREADS[1], f"C7 recovered RunTask ran in {THREADS[1:2]}"
