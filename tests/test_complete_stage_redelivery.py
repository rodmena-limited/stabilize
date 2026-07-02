"""
Repro tests for CompleteStage redelivery hazards (audit finding A1-2).

The after-stages and on-failure branches of CompleteStageHandler committed
state + StartStage pushes WITHOUT marking the source message processed in the
same transaction, and a redelivered/stale CompleteStage falling through the
status dispatch landed in the failure branch — cancelling a healthy workflow
whose synthetic stages (or tasks) were still running. The failure branch also
re-planned builder-defined on-failure stages on every delivery, duplicating
them.
"""

from typing import Any

from stabilize.handlers import CompleteStageHandler
from stabilize.models.stage import StageExecution, SyntheticStageOwner
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import CancelStage, CompleteStage, CompleteWorkflow
from stabilize.stages.builder import StageDefinitionBuilder, get_default_factory


def _drain(queue: Queue) -> list[Any]:
    messages = []
    while True:
        message = queue.poll_one()
        if message is None:
            break
        messages.append(message)
        queue.ack(message)
    return messages


def _task(status: WorkflowStatus) -> TaskExecution:
    task = TaskExecution.create(
        name="main task",
        implementing_class="noop",
        stage_start=True,
        stage_end=True,
    )
    task.status = status
    return task


def _store_running_workflow(repository: WorkflowStore, stages: list[StageExecution]) -> Workflow:
    execution = Workflow.create(application="test", name="redelivery test", stages=stages)
    execution.status = WorkflowStatus.RUNNING
    repository.store(execution)
    return execution


class TestAfterStageRedelivery:
    def test_after_stage_branch_marks_message_processed(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """The after-stages transaction must mark the source CompleteStage
        processed so a crash-before-ack redelivery is deduplicated."""
        parent = StageExecution(
            ref_id="parent",
            name="Parent",
            tasks=[_task(WorkflowStatus.SUCCEEDED)],
        )
        parent.status = WorkflowStatus.RUNNING
        after = StageExecution(
            ref_id="after",
            name="After",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_AFTER,
            tasks=[_task(WorkflowStatus.NOT_STARTED)],
        )
        after.parent_stage_id = parent.id
        execution = _store_running_workflow(repository, [parent, after])

        handler = CompleteStageHandler(queue, repository)
        message = CompleteStage(
            execution_type="PIPELINE",
            execution_id=execution.id,
            stage_id=parent.id,
            message_id="cs-after-1",
        )
        handler.handle(message)

        assert repository.is_message_processed("cs-after-1"), (
            "after-stages branch committed StartStage pushes without marking "
            "the source message processed"
        )

    def test_redelivery_while_after_stage_runs_does_not_cancel_workflow(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """A stale/redelivered CompleteStage while after-stages run must be a
        no-op, not cancel the workflow."""
        parent = StageExecution(
            ref_id="parent",
            name="Parent",
            tasks=[_task(WorkflowStatus.SUCCEEDED)],
        )
        parent.status = WorkflowStatus.RUNNING
        after = StageExecution(
            ref_id="after",
            name="After",
            synthetic_stage_owner=SyntheticStageOwner.STAGE_AFTER,
            tasks=[_task(WorkflowStatus.NOT_STARTED)],
        )
        after.parent_stage_id = parent.id
        execution = _store_running_workflow(repository, [parent, after])

        handler = CompleteStageHandler(queue, repository)
        message = CompleteStage(
            execution_type="PIPELINE",
            execution_id=execution.id,
            stage_id=parent.id,
            message_id="cs-after-2",
        )
        handler.handle(message)

        # Simulate the after-stage having started (StartStage processed).
        fresh = repository.retrieve(execution.id)
        after_fresh = next(s for s in fresh.stages if s.ref_id == "after")
        after_fresh.status = WorkflowStatus.RUNNING
        repository.store_stage(after_fresh)
        _drain(queue)

        handler.handle(message)  # redelivery (crash before ack)

        result = repository.retrieve(execution.id)
        parent_fresh = next(s for s in result.stages if s.ref_id == "parent")
        assert parent_fresh.status == WorkflowStatus.RUNNING, (
            f"parent wrongly transitioned to {parent_fresh.status}"
        )
        bad = [m for m in _drain(queue) if isinstance(m, (CancelStage, CompleteWorkflow))]
        assert bad == [], f"redelivery pushed premature terminal messages: {bad}"


class TestStaleCompleteStageWithRunningTasks:
    def test_stale_complete_stage_is_noop_while_tasks_run(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        """A CompleteStage arriving while the stage's tasks are still RUNNING
        (duplicate from a previous loop iteration) must not cancel anything."""
        parent = StageExecution(
            ref_id="parent",
            name="Parent",
            tasks=[_task(WorkflowStatus.RUNNING)],
        )
        parent.status = WorkflowStatus.RUNNING
        execution = _store_running_workflow(repository, [parent])

        handler = CompleteStageHandler(queue, repository)
        message = CompleteStage(
            execution_type="PIPELINE",
            execution_id=execution.id,
            stage_id=parent.id,
            message_id="cs-stale-1",
        )
        handler.handle(message)

        result = repository.retrieve(execution.id)
        parent_fresh = next(s for s in result.stages if s.ref_id == "parent")
        assert parent_fresh.status == WorkflowStatus.RUNNING, (
            f"stale CompleteStage wedged the stage into {parent_fresh.status}"
        )
        bad = [m for m in _drain(queue) if isinstance(m, (CancelStage, CompleteWorkflow))]
        assert bad == [], f"stale CompleteStage pushed premature terminal messages: {bad}"


class _RollbackOnFailureBuilder(StageDefinitionBuilder):
    """Test builder that plans one rollback stage when its stage fails."""

    @property
    def type(self) -> str:
        return "onfail_redelivery_demo"

    def on_failure_stages(self, stage: StageExecution, graph: Any) -> None:
        graph.add(
            StageExecution(
                ref_id=f"{stage.ref_id}_rollback",
                type="noop",
                name="Rollback",
                tasks=[_task(WorkflowStatus.NOT_STARTED)],
            )
        )


class TestOnFailureRedelivery:
    def test_on_failure_branch_marks_processed_and_never_duplicates(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        get_default_factory().register(_RollbackOnFailureBuilder())

        parent = StageExecution(
            ref_id="parent",
            type="onfail_redelivery_demo",
            name="Parent",
            tasks=[_task(WorkflowStatus.TERMINAL)],
        )
        parent.status = WorkflowStatus.RUNNING
        execution = _store_running_workflow(repository, [parent])

        handler = CompleteStageHandler(queue, repository)
        message = CompleteStage(
            execution_type="PIPELINE",
            execution_id=execution.id,
            stage_id=parent.id,
            message_id="cs-fail-1",
        )
        handler.handle(message)

        assert repository.is_message_processed("cs-fail-1"), (
            "on-failure branch committed StartStage pushes without marking "
            "the source message processed"
        )
        fresh = repository.retrieve(execution.id)
        rollbacks = [s for s in fresh.stages if s.ref_id == "parent_rollback"]
        assert len(rollbacks) == 1

        # Simulate the rollback stage having started, then redeliver.
        rollbacks[0].status = WorkflowStatus.RUNNING
        repository.store_stage(rollbacks[0])
        _drain(queue)

        handler.handle(message)

        result = repository.retrieve(execution.id)
        rollbacks = [s for s in result.stages if s.ref_id == "parent_rollback"]
        assert len(rollbacks) == 1, "redelivery duplicated on-failure stages"
        bad = [m for m in _drain(queue) if isinstance(m, (CancelStage, CompleteWorkflow))]
        assert bad == [], (
            f"redelivery completed the workflow while rollback was running: {bad}"
        )
