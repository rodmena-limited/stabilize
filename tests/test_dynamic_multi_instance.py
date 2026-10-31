"""WI-9 slice 9c (A11, B37): a dynamic multi-instance stage needs a builder for its instances, or it is refused."""

from __future__ import annotations

from datetime import timedelta

import pytest

from stabilize import StageExecution, TaskExecution, TaskResult
from stabilize.models.multi_instance import MultiInstanceConfig
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import AddMultiInstance
from stabilize.stages.builder import StageDefinitionBuilder, register_builder
from stabilize.stages.multi_instance_builder import MultiInstanceBuilder
from stabilize.tasks.interface import RetryableTask, Task
from tests.audit_support import drive, make_processor, start

RUNS: dict[str, int] = {}
RELEASED: set[str] = set()


class Hold(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(milliseconds=100)

    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.execution.id in RELEASED:
            return TaskResult.success()
        return TaskResult.running()


class Mark(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        RUNS[stage.ref_id] = RUNS.get(stage.ref_id, 0) + 1
        return TaskResult.success(outputs={"ran": stage.ref_id})


class DyninstanceStageBuilder(StageDefinitionBuilder):
    def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
        return [TaskExecution.create("mark", "mark", stage_start=True, stage_end=True)]


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    register_builder(DyninstanceStageBuilder())
    RUNS.clear()
    RELEASED.clear()


def _parent() -> StageExecution:
    return StageExecution(
        ref_id="mi",
        type="holdparent",
        name="mi",
        tasks=[TaskExecution.create("hold", "hold", stage_start=True, stage_end=True)],
    )


def test_create_dynamic_refuses_an_instance_type_without_a_builder() -> None:
    with pytest.raises(ValueError, match="nobuilder_xyz") as refused:
        MultiInstanceBuilder.create_dynamic(parent_stage=_parent(), instance_type="nobuilder_xyz")
    assert "register_builder" in str(refused.value)
    with pytest.raises(ValueError, match="holdparent"):
        MultiInstanceBuilder.create_dynamic(parent_stage=_parent())
    parent = _parent()
    assert MultiInstanceBuilder.create_dynamic(parent_stage=parent, instance_type="dyninstance") == []
    assert parent.mi_config is not None and parent.mi_config.instance_type == "dyninstance"
    assert MultiInstanceConfig.from_dict(parent.mi_config.to_dict()).instance_type == "dyninstance"


def test_an_added_instance_runs_the_tasks_its_builder_defines(repository: WorkflowStore, queue: Queue) -> None:
    processor = make_processor(
        repository, queue, {"hold": Hold, "mark": Mark}, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100
    )
    parent = _parent()
    MultiInstanceBuilder.create_dynamic(parent_stage=parent, instance_type="dyninstance", initial_count=0)
    wf = start(repository, queue, [parent])
    drive(processor, repository, wf.id, timeout=2, idle=0.5)
    assert repository.retrieve(wf.id).stages[0].status == WorkflowStatus.RUNNING
    queue.push(AddMultiInstance(execution_type=wf.type.value, execution_id=wf.id, stage_id=parent.id))
    drive(processor, repository, wf.id, timeout=4, idle=1.0)
    added = [s for s in repository.retrieve(wf.id).stages if s.ref_id.startswith("mi_instance_")]
    assert [s.ref_id for s in added] == ["mi_instance_1"]
    assert added[0].type == "dyninstance", "the instance did not take the type given to create_dynamic"
    RELEASED.add(wf.id)
    drive(processor, repository, wf.id, timeout=8, idle=1.5)
    loaded = repository.retrieve(wf.id)
    instance = next(s for s in loaded.stages if s.ref_id == "mi_instance_1")
    assert [t.name for t in instance.tasks] == ["mark"], (
        "the instance has no tasks; it would complete having run nothing"
    )
    assert RUNS.get("mi_instance_1") == 1, f"the instance's task never ran: {RUNS}"
    assert instance.status == WorkflowStatus.SUCCEEDED
    assert loaded.status == WorkflowStatus.SUCCEEDED


def test_an_instance_type_that_lost_its_builder_is_refused_and_recorded(
    repository: WorkflowStore, queue: Queue
) -> None:
    processor = make_processor(
        repository, queue, {"hold": Hold}, task_backoff_min_delay_ms=100, task_backoff_max_delay_ms=100
    )
    parent = _parent()
    parent.mi_config = MultiInstanceConfig(allow_dynamic=True, sync_on_complete=True, instance_type="vanished_type")
    wf = start(repository, queue, [parent])
    drive(processor, repository, wf.id, timeout=2, idle=0.5)
    queue.push(AddMultiInstance(execution_type=wf.type.value, execution_id=wf.id, stage_id=parent.id))
    drive(processor, repository, wf.id, timeout=4, idle=1.0)
    loaded = repository.retrieve(wf.id)
    assert [s.ref_id for s in loaded.stages if s.ref_id.startswith("mi_instance_")] == [], (
        "an instance was created for a type with no builder; it would complete having run nothing"
    )
    mi = next(s for s in loaded.stages if s.ref_id == "mi")
    refused = mi.context.get("_mi_dynamic_refused")
    assert refused is not None and refused["instance_type"] == "vanished_type"
    assert mi.context.get("_mi_instance_count", 0) == 0
    assert queue.dlq_size() == 0, "the refused AddMultiInstance was dead-lettered instead of acknowledged"
