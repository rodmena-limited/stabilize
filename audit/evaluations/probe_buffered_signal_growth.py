"""Reproduction and verification for unbounded _buffered_signals growth.

Uses ci-conductor's exact production payload: {"signal_name": "ci.resume", "signal_data": {}}.

Run: python audit/evaluations/probe_buffered_signal_growth.py [N]
"""

from __future__ import annotations

import json
import os
import sys

from stabilize import (
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    Task,
    TaskRegistry,
    TaskResult,
)
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import SignalStage

SIGNAL_NAME = "ci.resume"
SIGNAL_DATA: dict[str, object] = {}


class NoopTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"ok": True})


class SuspendTask(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        if stage.context.get("_signal_data") is not None:
            return TaskResult.success(outputs={"resumed": True})
        return TaskResult.suspend(context={"waiting_for": "signal"})


def _harness(task_name: str, task_cls: type) -> tuple:
    db = "sqlite:///:memory:"
    repo = SqliteWorkflowStore(connection_string=db, create_tables=True)
    queue = SqliteQueue(connection_string=db, table_name="queue_messages")
    queue._create_table()
    registry = TaskRegistry()
    registry.register(task_name, task_cls)
    processor = QueueProcessor(queue, store=repo, task_registry=registry)
    return repo, queue, processor, Orchestrator(queue)


SIBLING_KEYS = {
    "env": {"CI": "true"},
    "head_sha": "0" * 40,
    "job_id": "job-42",
    "script": "make test",
    "slug": "rodmena-limited/example",
    "secrets": ["NPM_TOKEN", "DEPLOY_KEY", "SENTRY_DSN"],
    "exception": None,
    "steps": ["build", "test"],
}


def _workflow(name: str, task_name: str) -> Workflow:
    return Workflow.create(
        application="probe",
        name=name,
        stages=[
            StageExecution(
                ref_id="only",
                name="Only",
                context=dict(SIBLING_KEYS),
                tasks=[TaskExecution.create("T", task_name, stage_start=True, stage_end=True)],
            )
        ],
    )


def _blast(queue, wf: Workflow, stage: StageExecution, n: int) -> None:
    for _ in range(n):
        queue.push(
            SignalStage(
                execution_type=wf.type.value,
                execution_id=wf.id,
                stage_id=stage.id,
                signal_name=SIGNAL_NAME,
                signal_data=dict(SIGNAL_DATA),
                persistent=True,
            )
        )


def case_terminal_stage(n: int) -> tuple[str, bool, str]:
    """A complete stage must never accumulate a buffer."""
    repo, queue, processor, runner = _harness("noop", NoopTask)
    wf = _workflow("terminal-stage", "noop")
    repo.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    stage = repo.retrieve(wf.id).stage_by_ref_id("only")
    assert stage.status == WorkflowStatus.SUCCEEDED, stage.status

    _blast(queue, wf, stage, n)
    processor.process_all(timeout=600.0)

    stage = repo.retrieve(wf.id).stage_by_ref_id("only")
    buffered = stage.context.get("_buffered_signals", [])
    size = len(json.dumps(stage.context, default=str).encode())
    survived = {k: stage.context.get(k) for k in SIBLING_KEYS}
    ok = len(buffered) == 0 and survived == SIBLING_KEYS
    detail = (
        f"status={stage.status} signals_sent={n} buffered={len(buffered)} "
        f"context={size}B sibling_keys_intact={survived == SIBLING_KEYS}"
    )
    return "complete stage refuses every persistent signal", ok, detail


def case_cap_enforced(n: int, cap: int) -> tuple[str, bool, str]:
    """A live-but-not-suspended stage must stop at the cap."""
    os.environ["STABILIZE_SIGNAL_BUFFER_MAX"] = str(cap)
    from stabilize.resilience.config import reset_handler_config

    reset_handler_config()

    repo, queue, processor, runner = _harness("suspend", SuspendTask)
    wf = _workflow("cap-enforced", "suspend")
    repo.store(wf)
    runner.start(wf)
    processor.process_one()

    stage = repo.retrieve(wf.id).stage_by_ref_id("only")
    _blast(queue, wf, stage, n)
    processor.process_all(timeout=600.0)

    stage = repo.retrieve(wf.id).stage_by_ref_id("only")
    buffered = stage.context.get("_buffered_signals", [])
    dlq = queue.dlq_size()
    expected_dlq = n - cap
    from stabilize.resilience.config import get_handler_config

    assert get_handler_config().signal_buffer_max == cap, "cap did not take effect"
    ok = len(buffered) <= cap and dlq == expected_dlq
    detail = (
        f"status={stage.status} signals_sent={n} cap={cap} "
        f"buffered={len(buffered)} dlq={dlq} expected_dlq={expected_dlq} "
        f"(cap admitted {n - dlq}, refused {dlq})"
    )
    return "buffer stops at the cap and overflow is dead-lettered", ok, detail


def case_drain_still_works() -> tuple[str, bool, str]:
    """A signal buffered before suspend must still resume the stage (WCP-24 unchanged)."""
    os.environ["STABILIZE_SIGNAL_BUFFER_MAX"] = "1000"
    from stabilize.resilience.config import reset_handler_config

    reset_handler_config()

    repo, queue, processor, runner = _harness("suspend", SuspendTask)
    wf = _workflow("drain-works", "suspend")
    repo.store(wf)
    runner.start(wf)
    processor.process_one()

    stage = repo.retrieve(wf.id).stage_by_ref_id("only")
    _blast(queue, wf, stage, 1)
    processor.process_all(timeout=60.0)

    stage = repo.retrieve(wf.id).stage_by_ref_id("only")
    detail = f"status={stage.status} buffered={len(stage.context.get('_buffered_signals', []))}"
    return "WCP-24 still resumes a stage from a buffered signal", stage.status == WorkflowStatus.SUCCEEDED, detail


def case_cleanup_reclaims() -> tuple[str, bool, str]:
    """cleanup_buffered_signals must strip a buffer already stranded in the database."""
    repo, queue, processor, runner = _harness("noop", NoopTask)
    wf = _workflow("cleanup", "noop")
    repo.store(wf)
    runner.start(wf)
    processor.process_all(timeout=30.0)

    stage = repo.retrieve(wf.id).stage_by_ref_id("only")
    stage.context["_buffered_signals"] = [
        {"signal_name": SIGNAL_NAME, "signal_data": dict(SIGNAL_DATA)} for _ in range(5000)
    ]
    repo.store_stage(stage)

    before = repo.retrieve(wf.id).stage_by_ref_id("only")
    seeded = len(before.context.get("_buffered_signals", []))
    if seeded != 5000:
        return "cleanup reclaims a stranded buffer", False, f"seed failed: {seeded} entries"

    rows = repo.cleanup_buffered_signals(only_complete=True)
    after = repo.retrieve(wf.id).stage_by_ref_id("only")
    remaining = after.context.get("_buffered_signals")
    survived = {k: after.context.get(k) for k in SIBLING_KEYS}
    ok = remaining is None and rows == 1 and survived == SIBLING_KEYS
    detail = (
        f"seeded={seeded} rows_updated={rows} remaining={remaining} "
        f"sibling_keys_intact={survived == SIBLING_KEYS} "
        f"surviving_keys={sorted(after.context)}"
    )
    return "cleanup reclaims a stranded buffer", ok, detail


def _run(label: str, fn, *args) -> tuple[str, bool, str]:
    try:
        return fn(*args)
    except Exception as exc:  # a missing method or a crash is a FAIL, not an abort
        return label, False, f"raised {type(exc).__name__}: {exc}"


def main(n: int) -> int:
    results = [
        _run("complete stage refuses every persistent signal", case_terminal_stage, n),
        _run("buffer stops at the cap and overflow is dead-lettered", case_cap_enforced, n, 100),
        _run("WCP-24 still resumes a stage from a buffered signal", case_drain_still_works),
        _run("cleanup reclaims a stranded buffer", case_cleanup_reclaims),
    ]
    print()
    failed = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}")
        print(f"       {detail}")
        failed += 0 if ok else 1
    print()
    print("VERDICT:", "ALL GREEN" if failed == 0 else f"{failed} FAILING")
    return failed


if __name__ == "__main__":
    sys.exit(main(int(sys.argv[1]) if len(sys.argv) > 1 else 500))
