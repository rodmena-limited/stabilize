"""Engine threads run in the contextvars context of the code that started them (#101 R5)."""

from __future__ import annotations

import time
from contextvars import ContextVar

from stabilize import StageExecution, TaskResult, Workflow
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task
from tests.audit_support import make_processor, stage, start

TENANT: ContextVar[str] = ContextVar("stabilize_test_tenant", default="unset")
SEEN: dict[str, str] = {}


class SeesContext(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        SEEN[stage.execution.id if stage.has_execution() else stage.ref_id] = TENANT.get()
        return TaskResult.success()


def _finished(repository: WorkflowStore, workflow_id: str, seconds: float = 30.0) -> Workflow:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        workflow = repository.retrieve(workflow_id)
        if workflow.status.is_complete:
            return workflow
        time.sleep(0.05)
    raise AssertionError(f"workflow {workflow_id} did not finish in {seconds} s")


def test_tasks_run_in_the_context_the_processor_was_started_in(
    file_repository: WorkflowStore, file_queue: Queue
) -> None:
    processor = make_processor(file_repository, file_queue, {"sees": SeesContext})
    token = TENANT.set("tenant-a")
    try:
        processor.start()
    finally:
        TENANT.reset(token)
    try:
        workflow = start(file_repository, file_queue, [stage("a", ["sees"])])
        final = _finished(file_repository, workflow.id)
    finally:
        processor.stop(wait=True)
    assert final.status == WorkflowStatus.SUCCEEDED
    seen = SEEN.get(workflow.id)
    assert seen == "tenant-a", f"the task saw context {seen!r}, not the context the processor was started in"


def test_two_processors_started_in_different_contexts_keep_them_apart(
    file_repository: WorkflowStore, file_queue: Queue
) -> None:
    processors = []
    for tenant in ("tenant-a", "tenant-b"):
        token = TENANT.set(tenant)
        try:
            processor = make_processor(file_repository, file_queue, {"sees": SeesContext})
            processor.start()
            processors.append(processor)
        finally:
            TENANT.reset(token)
    try:
        workflows = [start(file_repository, file_queue, [stage("a", ["sees"])]) for _ in range(6)]
        for workflow in workflows:
            assert _finished(file_repository, workflow.id).status == WorkflowStatus.SUCCEEDED
    finally:
        for processor in processors:
            processor.stop(wait=True)
    seen = {SEEN.get(workflow.id) for workflow in workflows}
    assert seen <= {"tenant-a", "tenant-b"}, f"tasks saw contexts {seen}, expected only the processors' own"
