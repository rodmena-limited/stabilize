"""Ticket #34: MULTI_MERGE (WCP-8) fires once per upstream completion.

It was implemented as a readiness predicate returning READY whenever any upstream
completed, with the firing bookkeeping delegated to a caller branch that was
never written — so the 2nd..Nth triggers were swallowed and the stage behaved as
an AND-join ignoring its parents.

Controls here are load-bearing: an over-firing implementation would break the
AND-join case, and the downstream assertion pins the documented limit
(multiplicity does not propagate) rather than leaving it implied.
"""

from __future__ import annotations

from collections import Counter

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
from stabilize.models.stage import JoinType
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue


class Mark(Task):
    def __init__(self) -> None:
        self.runs: Counter[str] = Counter()
        self.triggers: list[str] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        self.runs[stage.ref_id] += 1
        if stage.ref_id == "mm":
            self.triggers.append(str(stage.context.get("_mm_trigger", "")))
        return TaskResult.success(outputs={"n": self.runs[stage.ref_id]})


def _stage(ref: str, reqs: set[str] | None = None, **kw) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type="mark",
        name=ref,
        requisite_stage_ref_ids=reqs or set(),
        tasks=[TaskExecution.create(ref, "mark", stage_start=True, stage_end=True)],
        **kw,
    )


@pytest.fixture
def engine(repository: WorkflowStore, queue: Queue):
    mark = Mark()
    registry = TaskRegistry()
    registry.register("mark", mark)
    processor = QueueProcessor(queue, store=repository, task_registry=registry)
    runner = Orchestrator(queue, store=repository)
    yield repository, processor, runner, mark
    processor.stop(wait=True)


def _fan_in(join: JoinType, upstreams: tuple[str, ...] = ("a", "b", "c")) -> list[StageExecution]:
    return [
        _stage("root"),
        *[_stage(name, {"root"}) for name in upstreams],
        _stage("mm", set(upstreams), join_type=join),
        _stage("down", {"mm"}),
    ]


def _run(engine, name: str, stages: list[StageExecution]) -> Workflow:
    store, processor, runner, _mark = engine
    workflow = Workflow.create(application="mm", name=name, stages=stages)
    store.store(workflow)
    runner.start(workflow)
    processor.process_all(timeout=60.0)
    return store.retrieve(workflow.id)


def test_multi_merge_fires_once_per_upstream(engine) -> None:
    _store, _proc, _runner, mark = engine
    result = _run(engine, "mm", _fan_in(JoinType.MULTI_MERGE))

    assert mark.runs["mm"] == 3
    assert result.status == WorkflowStatus.SUCCEEDED


def test_each_upstream_is_consumed_exactly_once(engine) -> None:
    _store, _proc, _runner, mark = engine
    result = _run(engine, "mm", _fan_in(JoinType.MULTI_MERGE))

    mm = next(s for s in result.stages if s.ref_id == "mm")
    consumed = mm.context["_mm_consumed"]
    assert sorted(consumed) == ["a", "b", "c"]
    assert len(consumed) == len(set(consumed))
    assert sorted(t for t in mark.triggers if t) == ["a", "b", "c"]


def test_firings_are_archived(engine) -> None:
    _store, _proc, _runner, _mark = engine
    result = _run(engine, "mm", _fan_in(JoinType.MULTI_MERGE))

    mm = next(s for s in result.stages if s.ref_id == "mm")
    # Two archived plus the final firing living in stage.outputs.
    assert len(mm.context["_mm_firings"]) == 2
    assert mm.outputs["n"] == 3


def test_and_join_over_the_same_graph_still_fires_once(engine) -> None:
    """Control: an over-firing implementation would break every other join."""
    _store, _proc, _runner, mark = engine
    result = _run(engine, "and", _fan_in(JoinType.AND))

    assert mark.runs["mm"] == 1
    assert result.status == WorkflowStatus.SUCCEEDED


def test_multiplicity_does_not_propagate_downstream(engine) -> None:
    """Control: pins the documented limit rather than leaving it implied."""
    _store, _proc, _runner, mark = engine
    _run(engine, "mm", _fan_in(JoinType.MULTI_MERGE))

    assert mark.runs["down"] == 1


def test_firings_are_bounded_by_upstream_count(engine) -> None:
    """Termination: two upstreams must give exactly two firings, never more."""
    _store, _proc, _runner, mark = engine
    result = _run(engine, "mm2", _fan_in(JoinType.MULTI_MERGE, ("a", "b")))

    assert mark.runs["mm"] == 2
    assert result.status == WorkflowStatus.SUCCEEDED


def test_discriminator_is_unaffected(engine) -> None:
    """Control: DISCRIMINATOR shares the firing hook that was edited."""
    _store, _proc, _runner, mark = engine
    result = _run(engine, "disc", _fan_in(JoinType.DISCRIMINATOR))

    assert mark.runs["mm"] == 1
    assert result.status == WorkflowStatus.SUCCEEDED
