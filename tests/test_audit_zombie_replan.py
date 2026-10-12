from __future__ import annotations

from typing import Any

import pytest

from stabilize import StageExecution, TaskExecution, TaskResult, Workflow
from stabilize.models.stage import SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartStage
from stabilize.stages.builder import StageDefinitionBuilder, register_builder
from stabilize.tasks.interface import Task
from tests.audit_support import drive, make_processor, start

RUNS: dict[str, int] = {}


class N3Before(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        RUNS["before"] = RUNS.get("before", 0) + 1
        return TaskResult.success()


class N3Main(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        RUNS["main"] = RUNS.get("main", 0) + 1
        return TaskResult.success()


class N3ParentStageBuilder(StageDefinitionBuilder):
    def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
        return [TaskExecution.create("main", "n3main", stage_start=True, stage_end=True)]

    def before_stages(self, stage: StageExecution, graph: Any) -> None:
        before = StageExecution.create_synthetic(
            type="n3before", name="before", parent=stage, owner=SyntheticStageOwner.STAGE_BEFORE
        )
        graph.add(before)


class N3BeforeStageBuilder(StageDefinitionBuilder):
    def build_tasks(self, stage: StageExecution) -> list[TaskExecution]:
        return [TaskExecution.create("before", "n3before", stage_start=True, stage_end=True)]


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    register_builder(N3ParentStageBuilder())
    register_builder(N3BeforeStageBuilder())
    RUNS.clear()


def _run(repository: WorkflowStore, queue: Queue, zombie: bool) -> Workflow:
    processor = make_processor(repository, queue, {"n3before": N3Before, "n3main": N3Main})
    parent = StageExecution(ref_id="p", type="n3parent", name="p", context={})
    wf = start(repository, queue, [parent])
    handler = processor._handlers[StartStage]
    original = handler._plan_stage
    state = {"done": False}

    def plan(stage: StageExecution) -> None:
        if zombie and not state["done"] and stage.ref_id == "p":
            state["done"] = True
            handler.handle(StartStage(execution_type=wf.type.value, execution_id=wf.id, stage_id=stage.id))
        original(stage)

    handler._plan_stage = plan
    result = drive(processor, repository, wf.id, timeout=20, idle=1.5)
    if zombie:
        assert state["done"], "the zombie StartStage was never interleaved"
    return result


def test_zombie_replan_racing_the_original_runs_before_stage_once(repository: WorkflowStore, queue: Queue) -> None:
    result = _run(repository, queue, zombie=True)
    states = [(s.ref_id, s.name, s.status.name, [t.status.name for t in s.tasks]) for s in result.stages]
    assert RUNS.get("before") == 1, f"N3 before-stage task executed {RUNS.get('before')} times; {states}"
    assert RUNS.get("main") == 1, f"N3 zombie re-plan: main task executed {RUNS.get('main')} times; {states}"
    assert result.status == WorkflowStatus.SUCCEEDED, f"N3 workflow {result.status.name}"


def test_zombie_replan_control_without_interleaving(repository: WorkflowStore, queue: Queue) -> None:
    result = _run(repository, queue, zombie=False)
    assert RUNS == {"before": 1, "main": 1}
    assert result.status == WorkflowStatus.SUCCEEDED
