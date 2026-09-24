"""retrieve_stage loads upstream and synthetic stages on the connection it already holds (#54)."""

from __future__ import annotations

import pytest

from stabilize.models.stage import StageExecution
from stabilize.models.stage.enums import SyntheticStageOwner
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.pool_options import PoolOptions

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _task(name: str) -> TaskExecution:
    return TaskExecution.create(name=name, implementing_class="noop", stage_start=True, stage_end=True)


def _workflow() -> tuple[Workflow, StageExecution, StageExecution]:
    upstream = StageExecution(ref_id="up", type="noop", name="Up", tasks=[_task("u")])
    parent = StageExecution(
        ref_id="main", type="noop", name="Main", requisite_stage_ref_ids={"up"}, tasks=[_task("m")]
    )
    workflow = Workflow.create(application="single-conn", name="w", stages=[upstream, parent])
    before = StageExecution(
        ref_id="main-before",
        type="noop",
        name="Before",
        parent_stage_id=parent.id,
        synthetic_stage_owner=SyntheticStageOwner.STAGE_BEFORE,
        tasks=[_task("b1"), _task("b2")],
    )
    before.execution = workflow
    return workflow, parent, before


def test_retrieve_stage_completes_on_a_one_connection_pool(postgres_url: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    store = PostgresWorkflowStore(
        postgres_url, options=PoolOptions(min_size=1, max_size=1, acquire_timeout=3.0)
    )
    try:
        workflow, parent, before = _workflow()
        store.store(workflow)
        store.add_stage(before)

        loaded = store.retrieve_stage(parent.id)

        assert [t.name for t in loaded.tasks] == ["m"]
        by_ref = {s.ref_id: s for s in loaded.execution.stages}
        assert set(by_ref) == {"main", "up", "main-before"}
        assert by_ref["main-before"].parent_stage_id == parent.id
        assert [t.name for t in by_ref["main-before"].tasks] == ["b1", "b2"]
        assert by_ref["main-before"].execution is loaded.execution
        assert by_ref["up"].execution is loaded.execution
        assert [s.ref_id for s in loaded.synthetic_stages()] == ["main-before"]
    finally:
        store.close()


def test_a_stage_with_no_upstream_and_no_synthetic_children_loads_alone(postgres_url: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore

    store = PostgresWorkflowStore(
        postgres_url, options=PoolOptions(min_size=1, max_size=1, acquire_timeout=3.0)
    )
    try:
        workflow, _, _ = _workflow()
        store.store(workflow)
        loaded = store.retrieve_stage(workflow.stage_by_ref_id("up").id)
        assert [s.ref_id for s in loaded.execution.stages] == ["up"]
        assert [t.name for t in loaded.tasks] == ["u"]
    finally:
        store.close()
