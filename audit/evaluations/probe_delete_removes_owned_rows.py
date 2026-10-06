"""Deleting a workflow removes every row it owns (#67).

0.31.0's store.delete removed pipeline_executions (cascading to stages and
tasks) and left processed_messages, stage_claims, workflow_signals and the
queue/DLQ rows of the workflow behind, with no retention for some of them.

Observation is through the product's interface: is_message_processed,
pending_signal_count, queue size / list_dlq. stage_claims has no read API, so it
is the one table read directly (stated, read-only).

  A  after store.delete + queue.purge_workflow, every owned row of workflow X is gone
  B  CONTROL: workflow Y's rows in the same tables are untouched
  C  CONTROL: every row of X existed before the delete (the zeros are not vacuous)

    python audit/evaluations/probe_delete_removes_owned_rows.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)
sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402

from stabilize import StageExecution, TaskExecution, Workflow  # noqa: E402
from stabilize.queue.messages import StartWorkflow  # noqa: E402


def _wf() -> Workflow:
    return Workflow.create(
        application="probe",
        name="delete",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("t", "shell", stage_start=True, stage_end=True)],
            )
        ],
    )


def _claims(store, execution_id: str) -> int:
    if hasattr(store, "_pool"):
        with store._pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM stage_claims WHERE execution_id = %s", (execution_id,))
            return int(cur.fetchone()["n"])
    row = store._get_connection().execute(
        "SELECT count(*) FROM stage_claims WHERE execution_id = ?", (execution_id,)
    ).fetchone()
    return int(row[0])


def _seed(store, queue, wf: Workflow, tag: str) -> None:
    store.store(wf)
    store.mark_message_processed(message_id=f"m-{tag}", handler_type="StartStage", execution_id=wf.id)
    store.buffer_signal(wf.id, "s", "go", {"k": tag})
    with store.transaction(queue) as txn:
        txn.acquire_claim(wf.id, f"claim-{tag}", wf.stages[0].id)
    queue.push(StartWorkflow(execution_type="workflow", execution_id=wf.id))
    msg = queue.poll_one()
    assert msg is not None and msg.execution_id == wf.id
    queue.move_to_dlq(msg.message_id, error="probe")


def _state(store, queue, wf: Workflow, tag: str) -> dict[str, int]:
    dlq = [r for r in queue.list_dlq(limit=1000) if wf.id in str(r.get("payload"))]
    return {
        "exists": int(store.exists(wf.id)),
        "processed": int(store.is_message_processed(f"m-{tag}")),
        "signals": store.pending_signal_count(wf.id, "s"),
        "claims": _claims(store, wf.id),
        "dlq": len(dlq),
    }


def main() -> int:
    failures: list[str] = []
    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        with factory() as (store, queue):
            x, y = _wf(), _wf()
            _seed(store, queue, x, "x")
            _seed(store, queue, y, "y")
            for wf in (x, y):
                queue.push(StartWorkflow(execution_type="workflow", execution_id=wf.id))
            before_x = _state(store, queue, x, "x")
            size_before = queue.size()
            store.delete(x.id)
            purged = queue.purge_workflow(x.id) if hasattr(queue, "purge_workflow") else 0
            after_x = _state(store, queue, x, "x")
            after_y = _state(store, queue, y, "y")
            size_after = queue.size()
            seeded = all(v >= 1 for v in before_x.values())
            gone = all(v == 0 for v in after_x.values()) and size_after == size_before - 1 and purged == 2
            kept = all(v >= 1 for v in after_y.values())
            print(f"    {name} X before={before_x} after={after_x} purged={purged} queue {size_before}->{size_after}")
            print(f"    {name} Y after={after_y}")
            for label, ok in (("C seeded", seeded), ("A X removed", gone), ("B Y untouched", kept)):
                print(f"[{'PASS' if ok else 'FAIL'}] {name} {label}")
                if not ok:
                    failures.append(f"{name}/{label}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — delete removes the workflow's rows and only those")
    return 0


if __name__ == "__main__":
    sys.exit(main())
