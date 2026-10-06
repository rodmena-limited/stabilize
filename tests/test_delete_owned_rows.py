from __future__ import annotations

from stabilize import StageExecution, TaskExecution, Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.queue.messages import StartWorkflow


def _wf() -> Workflow:
    return Workflow.create(
        application="test",
        name="delete",
        stages=[
            StageExecution(
                ref_id="s",
                type="test",
                name="s",
                tasks=[TaskExecution.create("t", "success", stage_start=True, stage_end=True)],
            )
        ],
    )


def _seed(repository: WorkflowStore, queue: Queue, wf: Workflow, tag: str) -> None:
    repository.store(wf)
    repository.mark_message_processed(message_id=f"m-{tag}", handler_type="StartStage", execution_id=wf.id)
    repository.buffer_signal(wf.id, "s", "go", {"k": tag})
    queue.push(StartWorkflow(execution_type="workflow", execution_id=wf.id))
    msg = queue.poll_one()
    assert msg is not None
    queue.move_to_dlq(msg.message_id, error="test")  # type: ignore[attr-defined]


def test_delete_and_purge_remove_only_that_workflows_rows(repository: WorkflowStore, queue: Queue) -> None:
    x, y = _wf(), _wf()
    _seed(repository, queue, x, "x")
    _seed(repository, queue, y, "y")
    for wf in (x, y):
        queue.push(StartWorkflow(execution_type="workflow", execution_id=wf.id))

    repository.delete(x.id)
    assert queue.purge_workflow(x.id) == 2

    assert not repository.exists(x.id)
    assert not repository.is_message_processed("m-x")
    assert repository.pending_signal_count(x.id, "s") == 0
    assert repository.exists(y.id)
    assert repository.is_message_processed("m-y")
    assert repository.pending_signal_count(y.id, "s") == 1
    assert queue.size() == 1
    assert queue.dlq_size() == 1  # type: ignore[attr-defined]
