"""Neighbour-stage lookups read only the neighbours, whatever the workflow's size (#61)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


@contextmanager
def _statements(repository: Any, backend: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    seen: list[str] = []
    if backend == "sqlite":
        conn = repository._get_connection()
        conn.set_trace_callback(seen.append)
        try:
            yield seen
        finally:
            conn.set_trace_callback(None)
        return
    import psycopg

    real = psycopg.Cursor.execute

    def execute(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        seen.append(str(query))
        return real(self, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", execute)
    try:
        yield seen
    finally:
        monkeypatch.setattr(psycopg.Cursor, "execute", real)


def _chain(repository: Any, length: int) -> Workflow:
    stages = [
        StageExecution(
            ref_id=f"s{i}",
            type="noop",
            name=f"S{i}",
            requisite_stage_ref_ids=set() if i == 0 else {f"s{i - 1}"},
            tasks=[TaskExecution.create(name=f"t{i}", implementing_class="noop")],
        )
        for i in range(length)
    ]
    workflow = Workflow.create(application="neighbours", name=f"n{length}", stages=stages)
    repository.store(workflow)
    return workflow


def _lookups(repository: Any, workflow: Workflow) -> tuple[list[StageExecution], ...]:
    middle = workflow.stage_by_ref_id("s1")
    return (
        repository.get_upstream_stages(workflow.id, "s1"),
        repository.get_downstream_stages(workflow.id, "s1"),
        repository.get_synthetic_stages(workflow.id, middle.id),
    )


def test_the_control_counter_sees_statements(repository: Any, backend: str, monkeypatch: pytest.MonkeyPatch) -> None:
    workflow = _chain(repository, 3)
    with _statements(repository, backend, monkeypatch) as seen:
        repository.retrieve(workflow.id)
    assert any("pipeline_executions" in s for s in seen)


def test_lookups_do_not_read_the_workflow(repository: Any, backend: str, monkeypatch: pytest.MonkeyPatch) -> None:
    workflow = _chain(repository, 3)
    with _statements(repository, backend, monkeypatch) as seen:
        _lookups(repository, workflow)
    assert [s for s in seen if "pipeline_executions" in s] == []


def test_lookups_never_read_the_whole_stage_list(
    repository: Any, backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow = _chain(repository, 40)
    with _statements(repository, backend, monkeypatch) as seen:
        _lookups(repository, workflow)
    stage_reads = [" ".join(s.split()) for s in seen if "FROM stage_executions" in s]
    assert stage_reads
    unfiltered = [s for s in stage_reads if "ref_id" not in s and "parent_stage_id" not in s]
    assert unfiltered == []


def test_neighbours_still_carry_their_tasks(repository: Any) -> None:
    workflow = _chain(repository, 3)
    upstream, downstream, synthetic = _lookups(repository, workflow)
    assert [(s.ref_id, [t.name for t in s.tasks]) for s in upstream] == [("s0", ["t0"])]
    assert [(s.ref_id, [t.name for t in s.tasks]) for s in downstream] == [("s2", ["t2"])]
    assert synthetic == []
