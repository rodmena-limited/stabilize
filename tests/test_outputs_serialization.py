"""
Repro tests for non-primitive value serialization crashes (audit finding A3-17).

Commit f74f86b added default=str for stage.context, but stage.outputs,
workflow context, and task_exception_details still used bare json.dumps — a
task returning a datetime/Path/custom object in outputs crashed persistence
(TypeError) and forced task re-execution.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from stabilize.models.stage import StageExecution
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue


def _task() -> TaskExecution:
    return TaskExecution.create(
        name="t", implementing_class="noop", stage_start=True, stage_end=True
    )


class TestNonPrimitiveOutputs:
    def test_store_stage_with_non_primitive_outputs(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        stage = StageExecution(ref_id="s", name="S", tasks=[_task()])
        execution = Workflow.create(application="t", name="w", stages=[stage])
        repository.store(execution)

        stage.outputs = {
            "when": datetime(2026, 7, 1, tzinfo=UTC),
            "path": Path("/tmp/artifact.txt"),
            "count": 3,
        }
        repository.store_stage(stage)  # crashed with TypeError before the fix

        fresh = repository.retrieve(execution.id)
        outputs = fresh.stages[0].outputs
        assert outputs["count"] == 3
        assert isinstance(outputs["when"], str)  # str-coerced, same as context
        assert outputs["path"].endswith("artifact.txt")

    def test_transactional_store_stage_with_non_primitive_outputs(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        stage = StageExecution(ref_id="s", name="S", tasks=[_task()])
        execution = Workflow.create(application="t", name="w", stages=[stage])
        repository.store(execution)

        stage.outputs = {"when": datetime(2026, 7, 1, tzinfo=UTC)}
        with repository.transaction(queue) as txn:
            txn.store_stage(stage)

        fresh = repository.retrieve(execution.id)
        assert isinstance(fresh.stages[0].outputs["when"], str)

    def test_workflow_context_and_task_exception_details(
        self, repository: WorkflowStore, queue: Queue, backend: str
    ) -> None:
        task = _task()
        stage = StageExecution(ref_id="s", name="S", tasks=[task])
        execution = Workflow.create(application="t", name="w", stages=[stage])
        execution.context["started_at"] = datetime(2026, 7, 1, tzinfo=UTC)
        repository.store(execution)  # crashed with TypeError before the fix

        task.task_exception_details = {"raised_at": datetime(2026, 7, 1, tzinfo=UTC)}
        repository.store_stage(stage)

        fresh = repository.retrieve(execution.id)
        assert isinstance(fresh.context["started_at"], str)
        details: dict[str, Any] = fresh.stages[0].tasks[0].task_exception_details
        assert isinstance(details["raised_at"], str)
