"""Audit probe (C1): stage-level messages are not scoped to their workflow.

``StabilizeHandler.with_stage`` resolves ``message.stage_id`` by primary key and
never compares the resolved stage's workflow to ``message.execution_id``. Every
``StageLevel`` message therefore acts on whatever stage the id names, whichever
workflow it belongs to.

This probe drives the PUBLIC ``stabilize.hitl.approve`` API only.

  FINDING    an approve addressed to workflow B, carrying workflow A's stage
             id, releases workflow A's human-approval gate.

  CONTROL    an approve addressed to A's own execution id still releases A.
             Without this the probe could pass by breaking signals outright:
             a check that cannot go green cannot go red.

Exit 0 = both the control and the ownership check hold. Exit 1 = defect present.

Run:  python audit/evaluations/probe_stage_message_ownership.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)
from stabilize.hitl import ApprovalTask, approve  # noqa: E402


def _build(db: Path):
    store = SqliteWorkflowStore(f"sqlite:///{db}", create_tables=True)
    queue = SqliteQueue(f"sqlite:///{db}", table_name="queue_messages")
    queue._create_table()
    registry = TaskRegistry()
    registry.register("approval", ApprovalTask)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    runner = Orchestrator(queue, store=store)
    return store, queue, processor, runner


def _gate_workflow(name: str) -> Workflow:
    stage = StageExecution(
        ref_id="gate",
        type="approval",
        name="Gate",
        tasks=[
            TaskExecution.create(
                name="wait",
                implementing_class="approval",
                stage_start=True,
                stage_end=True,
            )
        ],
    )
    return Workflow.create(application="probe-c1", name=name, stages=[stage])


def _gate_of(store, wf_id: str) -> StageExecution:
    return store.retrieve(wf_id).stages[0]


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        store, queue, processor, runner = _build(Path(tmp) / "c1.db")

        wf_a = _gate_workflow("A")
        wf_b = _gate_workflow("B")
        for wf in (wf_a, wf_b):
            store.store(wf)
            runner.start(wf)
        processor.process_all(timeout=20.0)

        a_gate = _gate_of(store, wf_a.id)
        b_gate = _gate_of(store, wf_b.id)
        print(f"suspended: A={a_gate.status.name} B={b_gate.status.name}")
        if a_gate.status.name != "SUSPENDED" or b_gate.status.name != "SUSPENDED":
            print("INCONCLUSIVE: gates did not suspend; probe proves nothing")
            return 1

        # The attack: addressed to B's execution, carrying A's stage id.
        approve(queue, execution_id=wf_b.id, stage_id=a_gate.id, data={"by": "caller-in-B"})
        processor.process_all(timeout=20.0)

        a_after = _gate_of(store, wf_a.id)
        crossed = a_after.status.name != "SUSPENDED"
        print(f"after cross-workflow approve: A={a_after.status.name} outputs={a_after.outputs}")

        # The control: a correctly addressed approve must still work.
        b_gate = _gate_of(store, wf_b.id)
        approve(queue, execution_id=wf_b.id, stage_id=b_gate.id, data={"by": "caller-in-B"})
        processor.process_all(timeout=20.0)
        b_after = _gate_of(store, wf_b.id)
        control_ok = b_after.status.name == "SUCCEEDED"
        print(f"after same-workflow approve: B={b_after.status.name} outputs={b_after.outputs}")

        processor.stop(wait=True)

        print()
        if not control_ok:
            print("FAIL (control): a correctly addressed approve did not release its own gate.")
            print("      The ownership check below cannot be trusted while this is false.")
            return 1
        if crossed:
            print("FAIL (C1): an approve scoped to workflow B released workflow A's gate.")
            return 1
        print("PASS: stage-level messages are scoped to their own workflow.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
