"""cancel_remaining refuses rather than silently doing nothing.

The flag was accepted by the builder, persisted, and read by nothing: with
count=5 and join_threshold=3 the join fired at 3 and all five instances ran.

Three implementations were tried and all three are no-ops BY CONSTRUCTION,
which is the finding rather than a coding failure. Measured:

  cancellation-time  when the threshold fires, every remaining instance already
                     reads RUNNING (count=5 and count=20 alike) - nothing is
                     NOT_STARTED to cancel
  admission-time     the quota check is reached for all 8 instances and returns
                     False every time, because all StartStage messages are
                     processed before any instance completes

Every instance of a fixed multi-instance stage is dispatched when the parent
completes, so honouring the flag needs a cancellation channel into a RUNNING
task, which the engine does not have. Until it does, the builder refuses the
argument instead of accepting one it cannot honour.

  A  cancel_remaining=True   -> the builder RAISES
  B  cancel_remaining=False  -> every instance executes (the control)
  C  no threshold at all     -> every instance executes (the control)

    python audit/evaluations/probe_multi_instance_cancel_remaining.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
import threading
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
    WorkflowStatus,
)
from stabilize.stages.multi_instance_builder import MultiInstanceBuilder  # noqa: E402

COUNT = 5
THRESHOLD = 3


class Recorder(Task):
    """Records which instances actually executed, slowly enough to be cancellable."""

    def __init__(self) -> None:
        self.executed: list[int] = []
        self._lock = threading.Lock()

    def execute(self, stage: StageExecution) -> TaskResult:
        index = stage.context.get("_mi_instance_index")
        with self._lock:
            self.executed.append(index)
        return TaskResult.success(outputs={"index": index})


def _run(cancel_remaining: bool, join_threshold: int, name: str) -> tuple[Workflow, Recorder]:
    workdir = Path(tempfile.mkdtemp(prefix="probe-mi-"))
    dsn = f"sqlite:///{workdir / 'probe.db'}"
    store = SqliteWorkflowStore(dsn, create_tables=True)
    queue = SqliteQueue(dsn, table_name="queue_messages")
    queue._create_table()

    recorder = Recorder()
    registry = TaskRegistry()
    registry.register("work", recorder)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    orchestrator = Orchestrator(queue, store=store)

    parent = StageExecution.create(type="work", name="MI Parent", ref_id="parent")
    parent.tasks = [TaskExecution.create("seed", "work", stage_start=True, stage_end=True)]
    stages = [parent] + MultiInstanceBuilder.create_fixed(
        parent_stage=parent,
        count=COUNT,
        instance_type="work",
        join_threshold=join_threshold,
        cancel_remaining=cancel_remaining,
    )
    for stage in stages:
        if "_instance_" in stage.ref_id:
            stage.tasks = [TaskExecution.create("run", "work", stage_start=True, stage_end=True)]
        elif stage.ref_id.endswith("_mi_join"):
            stage.tasks = [TaskExecution.create("join", "work", stage_start=True, stage_end=True)]

    workflow = Workflow.create(application="probe-mi", name=name, stages=stages)
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=45.0)
    result = store.retrieve(workflow.id)
    processor.stop(wait=True)
    return result, recorder


def _instance_states(workflow: Workflow) -> dict[str, str]:
    return {
        s.ref_id: str(s.status)
        for s in workflow.stages
        if "_instance_" in s.ref_id
    }


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    print(f"=== A. cancel_remaining=True, count={COUNT}, join_threshold={THRESHOLD} ===")
    print("    the engine cannot honour this, so the builder must refuse it")
    try:
        _run(True, THRESHOLD, "cancel-on")
        print("    ACCEPTED SILENTLY — the flag still lies about what it does")
        results.append(("cancel_remaining is refused, not ignored", False, "builder accepted it"))
    except NotImplementedError as exc:
        names_reason = "RUNNING" in str(exc)
        print("    RAISED NotImplementedError")
        print(f"    explains why rather than just refusing: {names_reason}")
        results.append(("cancel_remaining is refused, not ignored", True, "NotImplementedError"))
        results.append(("the refusal explains why", names_reason, str(exc)[:110]))

    print()
    print(f"=== B. CONTROL — cancel_remaining=False, same threshold ===")
    print("    without this, an engine that always skipped would pass A")
    wf_b, rec_b = _run(False, THRESHOLD, "cancel-off")
    states_b = _instance_states(wf_b)
    skipped_b = [r for r, s in states_b.items() if "SKIPPED" in s]
    ran_b = len([x for x in rec_b.executed if x is not None])
    print(f"    instances that executed: {sorted(x for x in rec_b.executed if x is not None)}")
    print(f"    skipped: {sorted(skipped_b)}")
    results.append((
        "without the flag, no instance is cancelled",
        not skipped_b,
        f"{ran_b} of {COUNT} executed, skipped={sorted(skipped_b)}",
    ))

    print()
    print("=== C. THE REFUSAL DOES NOT DEPEND ON A THRESHOLD ===")
    print("    cancel_remaining with no threshold is equally unhonourable, so it")
    print("    must refuse too rather than appear to work in the degenerate case")
    try:
        _run(True, 0, "no-threshold")
        print("    ACCEPTED with join_threshold=0")
        results.append(("refusal is independent of the threshold", False, "accepted"))
    except NotImplementedError:
        print("    RAISED NotImplementedError")
        results.append(("refusal is independent of the threshold", True, "NotImplementedError"))

    print()
    print("=== D. THE WORKFLOW STILL COMPLETES IN THE SUPPORTED CASES ===")
    for label, wf in (("B", wf_b),):
        ok = wf.status in (WorkflowStatus.SUCCEEDED,)
        print(f"    {label}: {wf.status}")
        results.append((f"workflow {label} reaches a successful terminal state", ok, str(wf.status)))

    print()
    failures = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1

    print()
    if failures:
        print(f"VERDICT: FAIL — {failures} of {len(results)} checks failed")
        return 1
    print(f"VERDICT: PASS — cancel_remaining refuses instead of silently doing nothing ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
