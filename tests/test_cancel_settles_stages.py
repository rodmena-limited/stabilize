from __future__ import annotations

from stabilize import QueueProcessor, StageExecution, TaskExecution, TaskRegistry, Workflow, WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import CompleteWorkflow


def _stage(ref: str, requisites: set[str]) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type="test",
        name=ref,
        requisite_stage_ref_ids=requisites,
        tasks=[TaskExecution.create("t", "success", stage_start=True, stage_end=True)],
    )


def test_canceled_workflow_is_never_reported_with_a_runnable_stage(repository: WorkflowStore, queue: Queue) -> None:
    wf = Workflow.create(application="test", name="cancel", stages=[_stage("1", set()), _stage("2", {"1"})])
    wf.status = WorkflowStatus.RUNNING
    wf.stages[0].status = WorkflowStatus.CANCELED
    repository.store(wf)
    repository.cancel(wf.id, "tester", "test")

    queue.push(CompleteWorkflow(execution_type=wf.type.value, execution_id=wf.id))
    QueueProcessor(queue, store=repository, task_registry=TaskRegistry()).process_all(timeout=5.0)

    result = repository.retrieve(wf.id)
    assert result.status == WorkflowStatus.CANCELED
    assert {s.ref_id: s.status for s in result.stages}["2"] == WorkflowStatus.CANCELED
