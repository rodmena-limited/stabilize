from __future__ import annotations

import subprocess
import sys
import time
from datetime import timedelta

import pytest

from stabilize import StageExecution, TaskResult
from stabilize.models.status import WorkflowStatus
from stabilize.persistence.store import WorkflowStore
from stabilize.queue import Queue
from stabilize.tasks.interface import RetryableTask, Task
from tests.audit_support import drive, make_processor, stage, start


class Ok(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


class Hold(RetryableTask):
    def get_timeout(self) -> timedelta:
        return timedelta(minutes=5)

    def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
        return timedelta(minutes=5)

    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.running()


def test_purge_removes_completed_workflows_and_keeps_live_ones(
    repository: WorkflowStore, queue: Queue, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    processor = make_processor(repository, queue, {"ok": Ok, "hold": Hold})
    done = start(repository, queue, [stage("s", ["ok"])], application="purge-test")
    drive(processor, repository, done.id)
    live = start(repository, queue, [stage("s", ["hold"])], application="purge-test")
    drive(processor, repository, live.id, idle=0.5)
    assert repository.retrieve(done.id).status == WorkflowStatus.SUCCEEDED
    cutoff = int(time.time() * 1000) + 60_000
    try:
        removed = repository.purge_completed_workflows(cutoff, batch_size=10, application="purge-test")
    except (AttributeError, NotImplementedError) as exc:
        raise AssertionError(f"C15 no purge_completed_workflows on {type(repository).__name__}: {exc}") from exc
    assert removed == 1
    assert not repository.exists(done.id)
    assert repository.retrieve(live.id).status == WorkflowStatus.RUNNING


def test_prune_processed_cli_removes_old_records(file_repository: WorkflowStore, backend: str) -> None:
    repository = file_repository
    from stabilize.persistence.factory import create_queue

    q = create_queue(repository.connection_string, table_name="queue_messages")
    create = getattr(q, "_create_table", None)
    if callable(create):
        create()
    q.close()
    repository.mark_message_processed(message_id="987654321", handler_type="RunTask", execution_id=None)
    time.sleep(0.05)
    url = repository.connection_string
    result = subprocess.run(
        [sys.executable, "-m", "stabilize.cli.main", "prune-processed", "--db-url", url, "--older-than-hours", "0"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"C3 prune-processed CLI failed: {result.stderr[-300:]}"
    assert "Removed" in result.stdout
    assert not repository.is_message_processed("987654321")
