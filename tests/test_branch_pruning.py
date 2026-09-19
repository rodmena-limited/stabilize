"""Ticket #33: a de-selected branch must not execute.

An OR-split marked its non-activated children SKIPPED, but SKIPPED counts as
"upstream satisfied", so the rest of that branch ran anyway and a join whose
every branch was de-selected still fired.

The decision is made at each child's own readiness evaluation, where all of its
upstreams are visible. Every test here pairs the pruning with a control proving
a live branch is untouched — pruning everything would otherwise pass.
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
from stabilize.models.stage import JoinType, SplitType
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue


class Mark(Task):
    def __init__(self) -> None:
        self.ran: list[str] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        self.ran.append(stage.ref_id)
        return TaskResult.success(outputs={"from": stage.ref_id})


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


def _run(engine, name: str, stages: list[StageExecution]) -> Workflow:
    store, processor, runner, _mark = engine
    workflow = Workflow.create(application="prune", name=name, stages=stages)
    store.store(workflow)
    runner.start(workflow)
    processor.process_all(timeout=40.0)
    return store.retrieve(workflow.id)


def _status(workflow: Workflow, ref: str) -> WorkflowStatus:
    return next(s for s in workflow.stages if s.ref_id == ref).status


def test_deselected_branch_does_not_execute_two_deep(engine) -> None:
    _store, _proc, _runner, mark = engine
    result = _run(
        engine,
        "two-deep",
        [
            _stage("root", split_type=SplitType.OR,
                   split_conditions={"live": "True", "dead": "False"}),
            _stage("live", {"root"}),
            _stage("live2", {"live"}),
            _stage("dead", {"root"}),
            _stage("dead2", {"dead"}),
        ],
    )

    assert "dead2" not in mark.ran
    assert _status(result, "dead2") == WorkflowStatus.SKIPPED
    assert result.status == WorkflowStatus.SUCCEEDED


def test_selected_branch_still_runs_fully(engine) -> None:
    """Control: pruning must not touch the taken branch."""
    _store, _proc, _runner, mark = engine
    _run(
        engine,
        "two-deep",
        [
            _stage("root", split_type=SplitType.OR,
                   split_conditions={"live": "True", "dead": "False"}),
            _stage("live", {"root"}),
            _stage("live2", {"live"}),
            _stage("dead", {"root"}),
            _stage("dead2", {"dead"}),
        ],
    )

    assert "live" in mark.ran
    assert "live2" in mark.ran


def test_join_with_no_live_branch_is_pruned(engine) -> None:
    _store, _proc, _runner, mark = engine
    result = _run(
        engine,
        "all-pruned",
        [
            _stage("root", split_type=SplitType.OR,
                   split_conditions={"live": "True", "b": "False", "c": "False"}),
            _stage("live", {"root"}),
            _stage("b", {"root"}),
            _stage("c", {"root"}),
            _stage("orjoin", {"b", "c"}, join_type=JoinType.OR),
        ],
    )

    assert "orjoin" not in mark.ran
    assert _status(result, "orjoin") == WorkflowStatus.SKIPPED
    assert result.status == WorkflowStatus.SUCCEEDED


def test_join_with_one_live_branch_still_runs_once(engine) -> None:
    """Control: a child with any live edge carries a token and must run."""
    _store, _proc, _runner, mark = engine
    result = _run(
        engine,
        "diamond",
        [
            _stage("root", split_type=SplitType.OR,
                   split_conditions={"left": "True", "right": "False"}),
            _stage("left", {"root"}),
            _stage("right", {"root"}),
            _stage("join", {"left", "right"}),
        ],
    )

    assert mark.ran.count("join") == 1
    assert _status(result, "join") == WorkflowStatus.SUCCEEDED
    assert result.status == WorkflowStatus.SUCCEEDED


def test_plain_linear_workflow_is_unaffected(engine) -> None:
    """Control: a graph with no split carries no markers and must be untouched."""
    _store, _proc, _runner, mark = engine
    result = _run(
        engine,
        "linear",
        [_stage("a"), _stage("b", {"a"}), _stage("c", {"b"})],
    )

    assert mark.ran == ["a", "b", "c"]
    assert result.status == WorkflowStatus.SUCCEEDED
