"""Regression tests for the runflow evaluation findings 3-6.

Finding 3 (second half): stage_claims retention must delete only claims of
terminal executions. Finding 4: invalid stage graphs must be rejected at
Workflow.create(). Finding 5: a processor starting with recovery disabled
must say so. Finding 6: the recovery duplicate-guard must match the payload
task_id exactly (indexable) instead of a substring LIKE.
"""

from __future__ import annotations

import logging

import pytest

from stabilize.dag.topological import CircularDependencyError, InvalidStageGraphError
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import Workflow
from stabilize.persistence.sqlite import SqliteWorkflowStore
from stabilize.queue.messages import RunTask
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.sqlite import SqliteQueue


def _stage(ref_id: str, requisites: set[str] | None = None) -> StageExecution:
    stage = StageExecution(ref_id=ref_id, type="test", name=f"Stage {ref_id}", tasks=[])
    if requisites:
        stage.requisite_stage_ref_ids = requisites
    return stage


class TestSubmitTimeGraphValidation:
    def test_cycle_rejected_naming_members(self) -> None:
        with pytest.raises(CircularDependencyError) as exc:
            Workflow.create(
                application="t",
                name="cyclic",
                stages=[_stage("a", {"b"}), _stage("b", {"a"})],
            )
        assert "a" in str(exc.value) and "b" in str(exc.value)

    def test_self_edge_rejected_explicitly(self) -> None:
        with pytest.raises(InvalidStageGraphError, match="self_edge.*'a'"):
            Workflow.create(application="t", name="selfy", stages=[_stage("a", {"a"})])

    def test_unknown_ref_rejected_naming_the_typo(self) -> None:
        with pytest.raises(InvalidStageGraphError, match="unknown_ref.*'b'.*'nope'"):
            Workflow.create(
                application="t",
                name="typo",
                stages=[_stage("a"), _stage("b", {"nope"})],
            )

    def test_duplicate_ref_rejected(self) -> None:
        with pytest.raises(InvalidStageGraphError, match="duplicate_ref.*'a'"):
            Workflow.create(application="t", name="dupes", stages=[_stage("a"), _stage("a")])

    def test_valid_diamond_still_accepted(self) -> None:
        wf = Workflow.create(
            application="t",
            name="diamond",
            stages=[
                _stage("a"),
                _stage("b", {"a"}),
                _stage("c", {"a"}),
                _stage("d", {"b", "c"}),
            ],
        )
        assert len(wf.stages) == 4


class TestRecoveryDisabledWarning:
    def test_start_without_recovery_warns(self, caplog: pytest.LogCaptureFixture, tmp_path) -> None:
        url = f"sqlite:///{tmp_path}/warn.db"
        queue = SqliteQueue(url)
        queue._create_table()
        store = SqliteWorkflowStore(url, create_tables=True)
        processor = QueueProcessor(queue, store=store)

        with caplog.at_level(logging.WARNING, logger="stabilize.queue.processor.processor"):
            processor.start()
            processor.stop()
        assert any("Crash recovery is disabled" in r.message for r in caplog.records)

    def test_start_with_recovery_does_not_warn(self, caplog: pytest.LogCaptureFixture, tmp_path) -> None:
        from stabilize.queue.processor import QueueProcessorConfig

        url = f"sqlite:///{tmp_path}/nowarn.db"
        queue = SqliteQueue(url)
        queue._create_table()
        store = SqliteWorkflowStore(url, create_tables=True)
        processor = QueueProcessor(queue, store=store, config=QueueProcessorConfig(recover_on_start=True))

        with caplog.at_level(logging.WARNING, logger="stabilize.queue.processor.processor"):
            processor.start()
            processor.stop()
        assert not any("Crash recovery is disabled" in r.message for r in caplog.records)


class TestPendingTaskLookup:
    def test_exact_match_both_directions(self, queue) -> None:
        queue.push(
            RunTask(
                execution_type="PIPELINE",
                execution_id="exec-1",
                stage_id="stage-1",
                task_id="task-abc-123",
            )
        )
        assert queue.has_pending_message_for_task("task-abc-123") is True
        assert queue.has_pending_message_for_task("task-abc") is False, "substring must not match"
        assert queue.has_pending_message_for_task("other-task") is False


class TestStageClaimsRetention:
    def _make_workflow(self, store: SqliteWorkflowStore, status: WorkflowStatus) -> Workflow:
        wf = Workflow.create(application="t", name=f"wf-{status.name}", stages=[_stage("a")])
        wf.status = status
        store.store(wf)
        return wf

    def test_sweep_removes_only_terminal_execution_claims(self, tmp_path) -> None:
        url = f"sqlite:///{tmp_path}/claims.db"
        queue = SqliteQueue(url)
        queue._create_table()
        store = SqliteWorkflowStore(url, create_tables=True)

        done = self._make_workflow(store, WorkflowStatus.SUCCEEDED)
        live = self._make_workflow(store, WorkflowStatus.RUNNING)

        with store.transaction(queue) as txn:
            assert txn.acquire_claim(done.id, "mutex:m", "stage-done-1")
            assert txn.acquire_claim(live.id, "mutex:m", "stage-live-1")

        removed = store.cleanup_completed_stage_claims()
        assert removed == 1

        with store.transaction(queue) as txn:
            # Terminal execution's claim is gone: a new claimant wins it.
            assert txn.acquire_claim(done.id, "mutex:m", "stage-done-2") is True
            # Live execution's claim survived: a rival claimant still loses.
            assert txn.acquire_claim(live.id, "mutex:m", "stage-live-2") is False
