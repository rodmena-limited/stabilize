from __future__ import annotations

import pytest
from audit_support import drive, make_processor, stage, start

from stabilize import StageExecution, TaskResult
from stabilize.error_codes import classify_error
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import Task


class Raises(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        raise ValueError("bad input 4711")


def test_task_exception_keeps_its_own_code_and_text(
    repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    processor = make_processor(repository, queue, {"raises": Raises})
    wf = start(repository, queue, [stage("s", ["raises"])])
    result = drive(processor, repository, wf.id)
    details = (result.stages[0].context.get("exception") or {}).get("details") or {}
    assert "4711" in str(details.get("error"))
    want = classify_error(ValueError("x")).value
    assert details.get("error_code") == want, f"C10 error_code={details.get('error_code')}"
    assert not str(details.get("error")).startswith("Execution failed:")
