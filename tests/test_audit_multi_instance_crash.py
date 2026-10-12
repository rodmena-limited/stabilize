from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from stabilize import StageExecution, TaskExecution, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import AddMultiInstance
from stabilize.stages.builder import StageDefinitionBuilder, register_builder
from stabilize.stages.multi_instance_builder import MultiInstanceBuilder
from stabilize.tasks.interface import RetryableTask, Task
from tests.audit_support import drive, make_processor, start

RUNS: dict[str, int] = {}


class Hold(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()


class MiTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        RUNS[stage.ref_id] = RUNS.get(stage.ref_id, 0) + 1
        return TaskResult.success()


class MiInstanceStageBuilder(StageDefinitionBuilder):
    def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
        return [TaskExecution.create("mi", "mitask", stage_start=True, stage_end=True)]


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    register_builder(MiInstanceStageBuilder())
    RUNS.clear()


def _run(repository: WorkflowStore, queue: Queue, crash: bool) -> tuple[Any, list[StageExecution]]:
    processor = make_processor(
        repository,
        queue,
        {"hold": Hold, "mitask": MiTask},
        task_backoff_min_delay_ms=100,
        task_backoff_max_delay_ms=100,
    )
    parent = StageExecution(
        ref_id="mi",
        type="test",
        name="mi",
        context={},
        tasks=[TaskExecution.create("hold", "hold", stage_start=True, stage_end=True)],
    )
    MultiInstanceBuilder.create_dynamic(parent_stage=parent, instance_type="miinstance", initial_count=0)
    wf = start(repository, queue, [parent])
    drive(processor, repository, wf.id, timeout=2, idle=0.5)
    assert repository.retrieve(wf.id).stages[0].status == WorkflowStatus.RUNNING
    original = repository.add_stage
    state = {"raised": False}

    def add_stage(stage: StageExecution) -> None:
        if crash and not state["raised"]:
            state["raised"] = True
            raise ConnectionError("server closed the connection unexpectedly")
        original(stage)

    repository.add_stage = add_stage  # type: ignore[method-assign]
    queue.push(AddMultiInstance(execution_type=wf.type.value, execution_id=wf.id, stage_id=parent.id))
    drive(processor, repository, wf.id, timeout=6, idle=2.0)
    result = repository.retrieve(wf.id)
    instances = [s for s in result.stages if s.ref_id.startswith("mi_instance_")]
    return next(s for s in result.stages if s.ref_id == "mi"), instances


def test_add_instance_interrupted_after_its_commit_still_adds_it(repository: WorkflowStore, queue: Queue) -> None:
    parent, instances = _run(repository, queue, crash=True)
    count = parent.context.get("_mi_instance_count")
    found = [(s.ref_id, s.status.name) for s in instances]
    assert len(instances) == count, f"MI interrupted add: parent counts {count} instance(s), store has {found}"


def test_add_instance_control_without_fault(repository: WorkflowStore, queue: Queue) -> None:
    parent, instances = _run(repository, queue, crash=False)
    assert parent.context.get("_mi_instance_count") == 1
    assert [s.ref_id for s in instances] == ["mi_instance_1"]
