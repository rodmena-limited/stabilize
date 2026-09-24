"""store_stage writes only the task rows that changed; concurrency guarantees hold (#61)."""

from __future__ import annotations

from typing import Any

import pytest

from stabilize.errors import ConcurrencyError
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _task(name: str) -> TaskExecution:
    return TaskExecution.create(name=name, implementing_class="noop")


def _stored_stage(repository: Any) -> str:
    stage = StageExecution(ref_id="s", type="noop", name="S", tasks=[_task("a"), _task("b")])
    workflow = Workflow.create(application="dirty", name="w", stages=[stage])
    repository.store(workflow)
    return stage.id


def _versions(repository: Any, stage_id: str) -> dict[str, int]:
    return {t.name: t.version for t in repository.retrieve_stage(stage_id).tasks}


def _statuses(repository: Any, stage_id: str) -> dict[str, WorkflowStatus]:
    return {t.name: t.status for t in repository.retrieve_stage(stage_id).tasks}


def test_storing_a_stage_leaves_unchanged_task_rows_alone(repository: Any) -> None:
    stage_id = _stored_stage(repository)
    before = _versions(repository, stage_id)

    stage = repository.retrieve_stage(stage_id)
    stage.context["touched"] = True
    repository.store_stage(stage)

    assert _versions(repository, stage_id) == before
    assert repository.retrieve_stage(stage_id).context["touched"] is True


def test_only_the_changed_task_is_written(repository: Any) -> None:
    stage_id = _stored_stage(repository)
    before = _versions(repository, stage_id)

    stage = repository.retrieve_stage(stage_id)
    stage.tasks[0].status = WorkflowStatus.RUNNING
    repository.store_stage(stage)

    after = _versions(repository, stage_id)
    assert after["a"] == before["a"] + 1
    assert after["b"] == before["b"]
    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.RUNNING
    assert stage.tasks[0].version == after["a"]


def test_a_task_changed_and_changed_back_is_persisted_both_times(repository: Any) -> None:
    stage_id = _stored_stage(repository)
    stage = repository.retrieve_stage(stage_id)

    stage.tasks[0].status = WorkflowStatus.RUNNING
    repository.store_stage(stage)
    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.RUNNING

    stage.tasks[0].status = WorkflowStatus.NOT_STARTED
    repository.store_stage(stage)
    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.NOT_STARTED


def test_a_rolled_back_task_write_is_sent_again_and_versions_are_restored(repository: Any) -> None:
    stage_id = _stored_stage(repository)
    stage = repository.retrieve_stage(stage_id)
    stage_version = stage.version
    task_versions = [t.version for t in stage.tasks]

    stage.tasks[0].status = WorkflowStatus.RUNNING
    with pytest.raises(RuntimeError):
        with repository.transaction() as txn:
            txn.store_stage(stage)
            raise RuntimeError("abort")

    assert stage.version == stage_version
    assert [t.version for t in stage.tasks] == task_versions
    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.NOT_STARTED

    repository.store_stage(stage)
    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.RUNNING


def test_a_change_made_after_the_write_but_before_commit_is_sent_later(repository: Any) -> None:
    stage_id = _stored_stage(repository)
    stage = repository.retrieve_stage(stage_id)

    with repository.transaction() as txn:
        stage.tasks[0].status = WorkflowStatus.RUNNING
        txn.store_stage(stage)
        stage.tasks[0].status = WorkflowStatus.SUCCEEDED

    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.RUNNING
    repository.store_stage(stage)
    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.SUCCEEDED


def test_a_failed_store_restores_in_memory_versions(repository: Any) -> None:
    stage_id = _stored_stage(repository)
    winner = repository.retrieve_stage(stage_id)
    loser = repository.retrieve_stage(stage_id)

    winner.tasks[0].status = WorkflowStatus.RUNNING
    repository.store_stage(winner)

    loser.version = repository.retrieve_stage(stage_id).version
    loser_stage_version = loser.version
    loser_task_versions = [t.version for t in loser.tasks]
    loser.context["late"] = True
    loser.tasks[0].status = WorkflowStatus.TERMINAL
    with pytest.raises(ConcurrencyError, match="(?i)task"):
        repository.store_stage(loser)
    assert loser.version == loser_stage_version
    assert [t.version for t in loser.tasks] == loser_task_versions
    reloaded = repository.retrieve_stage(stage_id)
    assert "late" not in reloaded.context
    assert reloaded.version == loser_stage_version


@pytest.mark.parametrize("loser_touches_tasks", [False, True], ids=["stage-only", "task-too"])
def test_a_stale_writer_is_rejected_and_succeeds_after_reloading(repository: Any, loser_touches_tasks: bool) -> None:
    stage_id = _stored_stage(repository)
    winner = repository.retrieve_stage(stage_id)
    loser = repository.retrieve_stage(stage_id)

    winner.tasks[0].status = WorkflowStatus.RUNNING
    repository.store_stage(winner)

    loser.context["late"] = True
    if loser_touches_tasks:
        loser.tasks[0].status = WorkflowStatus.TERMINAL
    with pytest.raises(ConcurrencyError):
        repository.store_stage(loser)
    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.RUNNING

    fresh = repository.retrieve_stage(stage_id)
    fresh.context["late"] = True
    fresh.tasks[0].status = WorkflowStatus.SUCCEEDED
    repository.store_stage(fresh)
    reloaded = repository.retrieve_stage(stage_id)
    assert reloaded.context["late"] is True
    assert _statuses(repository, stage_id)["a"] == WorkflowStatus.SUCCEEDED


def test_a_loaded_stage_re_added_after_removal_gets_its_task_rows_back(repository: Any) -> None:
    stage_id = _stored_stage(repository)
    stage = repository.retrieve_stage(stage_id)
    repository.remove_stage(stage.execution, stage_id)

    repository.add_stage(stage)

    assert sorted(t.name for t in repository.retrieve_stage(stage_id).tasks) == ["a", "b"]
