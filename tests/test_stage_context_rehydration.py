"""Ticket #27: a re-entered stage must observe fresh upstream outputs.

``_plan_stage`` persists the merged context back onto the stage, so before this
fix the first value a stage observed for a key shadowed every later ancestor
output. Every test pairs the correction with a control asserting that ordinary
single-plan hydration and caller-set keys are unchanged.
"""

from __future__ import annotations

import pytest

from stabilize import (
    Orchestrator,
    QueueProcessor,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue

TARGET = 3


class Worker(Task):
    def __init__(self) -> None:
        self.runs = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        self.runs += 1
        return TaskResult.success(outputs={"attempts": self.runs})


class Checker(Task):
    def __init__(self) -> None:
        self.seen: list[object] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        value = stage.context.get("attempts")
        self.seen.append(value)
        if isinstance(value, int) and value >= TARGET:
            return TaskResult.success(outputs={"converged": True})
        return TaskResult.jump_to("work")


class Observer(Task):
    def __init__(self) -> None:
        self.seen: dict[str, object] = {}

    def execute(self, stage: StageExecution) -> TaskResult:
        self.seen = dict(stage.context)
        return TaskResult.success()


def _stage(ref: str, impl: str, reqs: set[str] | None = None, **kw) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type=impl,
        name=ref,
        requisite_stage_ref_ids=reqs or set(),
        tasks=[TaskExecution.create(ref, impl, stage_start=True, stage_end=True)],
        **kw,
    )


@pytest.fixture
def engine(repository: WorkflowStore, queue: Queue):
    tasks = {"work": Worker(), "check": Checker(), "observe": Observer()}
    registry = TaskRegistry()
    for name, impl in tasks.items():
        registry.register(name, impl)
    processor = QueueProcessor(queue, store=repository, task_registry=registry)
    runner = Orchestrator(queue, store=repository)
    yield repository, queue, processor, runner, tasks
    processor.stop(wait=True)


def _run(store, runner, processor, workflow) -> Workflow:
    store.store(workflow)
    runner.start(workflow)
    processor.process_all(timeout=30.0)
    return store.retrieve(workflow.id)


def test_reentered_stage_sees_fresh_upstream_output(engine) -> None:
    store, _q, processor, runner, tasks = engine
    result = _run(
        store,
        runner,
        processor,
        Workflow.create(
            application="c27",
            name="retry",
            stages=[_stage("work", "work"), _stage("check", "check", {"work"})],
        ),
    )

    assert tasks["check"].seen == [1, 2, 3]
    assert tasks["work"].runs == TARGET
    assert result.status == WorkflowStatus.SUCCEEDED


def test_caller_set_context_key_still_wins(engine) -> None:
    """Control: a key no ancestor publishes must survive hydration."""
    store, _q, processor, runner, tasks = engine
    _run(
        store,
        runner,
        processor,
        Workflow.create(
            application="c27",
            name="caller-key",
            stages=[
                _stage("work", "work"),
                _stage("observe", "observe", {"work"}, context={"caller_key": "mine"}),
            ],
        ),
    )

    assert tasks["observe"].seen["caller_key"] == "mine"


def test_caller_context_overrides_ancestor_on_first_plan(engine) -> None:
    """Control: single-plan precedence is unchanged — stage context wins."""
    store, _q, processor, runner, tasks = engine
    _run(
        store,
        runner,
        processor,
        Workflow.create(
            application="c27",
            name="override",
            stages=[
                _stage("work", "work"),
                _stage("observe", "observe", {"work"}, context={"attempts": "pinned"}),
            ],
        ),
    )

    assert tasks["observe"].seen["attempts"] == "pinned"


def test_ancestor_output_is_hydrated_normally(engine) -> None:
    """Control: ordinary hydration still delivers upstream outputs."""
    store, _q, processor, runner, tasks = engine
    _run(
        store,
        runner,
        processor,
        Workflow.create(
            application="c27",
            name="hydrate",
            stages=[_stage("work", "work"), _stage("observe", "observe", {"work"})],
        ),
    )

    assert tasks["observe"].seen["attempts"] == 1
