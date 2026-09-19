"""Ticket #31: structured loops (WCP-21) must actually execute.

LoopBuilder emitted stages referencing LoopConditionTask and LoopBackTask, which
existed nowhere, so every loop died on TaskNotFoundError at its first condition
check. These tests drive the documented builder API end to end.

Nested loops are covered too (#32): the inner loop must get a full, independent
budget on every pass of the outer loop.
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
from stabilize.stages.loop_builder import LoopBuilder


class Counter(Task):
    def __init__(self) -> None:
        self.seen: list[int] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        value = int(stage.context.get("i", 0) or 0)
        self.seen.append(value)
        return TaskResult.success(outputs={"i": value + 1})


def _body() -> StageExecution:
    return StageExecution(
        ref_id="body",
        type="body",
        name="body",
        tasks=[TaskExecution.create("body", "body", stage_start=True, stage_end=True)],
    )


@pytest.fixture
def engine(repository: WorkflowStore, queue: Queue):
    counter = Counter()
    registry = TaskRegistry()
    registry.register("body", counter)
    processor = QueueProcessor(queue, store=repository, task_registry=registry)
    runner = Orchestrator(queue, store=repository)
    yield repository, processor, runner, counter
    processor.stop(wait=True)


def _run(engine, stages, name: str) -> Workflow:
    store, processor, runner, _counter = engine
    workflow = Workflow.create(application="loops", name=name, stages=stages)
    store.store(workflow)
    runner.start(workflow)
    processor.process_all(timeout=60.0)
    return store.retrieve(workflow.id)


def _stage_by_ref(workflow: Workflow, ref: str) -> StageExecution:
    return next(s for s in workflow.stages if s.ref_id == ref)


def test_builtin_loop_tasks_are_registered_out_of_the_box() -> None:
    registry = TaskRegistry()
    for name in ("LoopConditionTask", "LoopBackTask", "LoopEntryTask", "WaitTask", "NoOpTask"):
        assert registry.has(name), name
    assert registry.has("wait")
    assert registry.has("noop")


def test_user_registration_still_overrides_a_builtin() -> None:
    """Control: seeding must not take a name away from a caller."""

    class Mine(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            return TaskResult.success()

    registry = TaskRegistry()
    registry.register("WaitTask", Mine)
    assert isinstance(registry.get("WaitTask"), Mine)


def test_while_loop_runs_exactly_n_iterations(engine) -> None:
    _store, _proc, _runner, counter = engine
    result = _run(
        engine,
        LoopBuilder.while_loop("i < 3", [_body()], "L", 10, {"i": 0}),
        "while",
    )

    assert counter.seen == [0, 1, 2]
    assert _stage_by_ref(result, "L_loopback").status == WorkflowStatus.SUCCEEDED
    assert result.status == WorkflowStatus.SUCCEEDED


def test_while_loop_with_false_condition_never_runs_the_body(engine) -> None:
    _store, _proc, _runner, counter = engine
    result = _run(
        engine,
        LoopBuilder.while_loop("i < 0", [_body()], "Z", 10, {"i": 0}),
        "zero",
    )

    assert counter.seen == []
    assert result.status == WorkflowStatus.SUCCEEDED


def test_loop_exhaustion_continues_past_the_loop(engine) -> None:
    _store, _proc, _runner, counter = engine
    result = _run(
        engine,
        LoopBuilder.while_loop("i < 999", [_body()], "E", 4, {"i": 0}),
        "exhaust",
    )

    assert len(counter.seen) == 4
    loopback = _stage_by_ref(result, "E_loopback")
    assert loopback.status == WorkflowStatus.FAILED_CONTINUE
    assert loopback.outputs["loop_exhausted"] is True
    assert result.status == WorkflowStatus.SUCCEEDED


def test_repeat_until_runs_the_body_at_least_once(engine) -> None:
    _store, _proc, _runner, counter = engine
    result = _run(
        engine,
        LoopBuilder.repeat_until("i >= 2", [_body()], "R", 10, {"i": 0}),
        "repeat",
    )

    assert counter.seen == [0, 1]
    assert result.status == WorkflowStatus.SUCCEEDED


def test_repeat_until_satisfied_immediately_still_runs_once(engine) -> None:
    _store, _proc, _runner, counter = engine
    _run(
        engine,
        LoopBuilder.repeat_until("i >= 0", [_body()], "R1", 10, {"i": 0}),
        "repeat-once",
    )

    assert counter.seen == [0]


def test_undefined_identifier_in_condition_fails_loudly(engine) -> None:
    """A condition nothing publishes must not silently spin to the bound."""
    _store, _proc, _runner, counter = engine
    result = _run(
        engine,
        LoopBuilder.while_loop("tests_passed == True", [_body()], "U", 5, {"i": 0}),
        "undefined",
    )

    condition = _stage_by_ref(result, "U_condition")
    assert condition.status == WorkflowStatus.TERMINAL
    assert counter.seen == []


class Bump(Task):
    """Advances the outer counter and re-arms the inner one."""

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(
            outputs={"o": int(stage.context.get("o", 0) or 0) + 1, "i": 0}
        )


@pytest.mark.parametrize(("outer_n", "inner_n"), [(3, 2), (2, 3), (1, 4)])
def test_nested_loops_run_the_inner_body_on_every_outer_pass(
    repository: WorkflowStore, queue: Queue, outer_n: int, inner_n: int
) -> None:
    counter = Counter()
    registry = TaskRegistry()
    registry.register("body", counter)
    registry.register("bump", Bump())
    processor = QueueProcessor(queue, store=repository, task_registry=registry)
    runner = Orchestrator(queue, store=repository)
    try:
        # The inner bound is deliberately tight. A loop-back that kept counting
        # from its own stale copy across outer passes would reach it early and
        # cut the last pass short; a generous bound would hide that entirely.
        inner = LoopBuilder.while_loop(
            f"i < {inner_n}", [_body()], "IN", inner_n + 1, {"i": 0}
        )
        bump = StageExecution(
            ref_id="bump",
            type="bump",
            name="bump",
            tasks=[TaskExecution.create("bump", "bump", stage_start=True, stage_end=True)],
        )
        stages = LoopBuilder.while_loop(
            f"o < {outer_n}", [*inner, bump], "OUT", outer_n + 1, {"o": 0, "i": 0}
        )
        workflow = Workflow.create(application="loops", name="nested", stages=stages)
        repository.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=120.0)
        result = repository.retrieve(workflow.id)

        assert len(counter.seen) == outer_n * inner_n
        assert counter.seen == list(range(inner_n)) * outer_n
        assert result.status == WorkflowStatus.SUCCEEDED
    finally:
        processor.stop(wait=True)
