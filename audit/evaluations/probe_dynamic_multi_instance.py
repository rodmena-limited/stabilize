"""create_dynamic(initial_count > 0) must still allow later instances (WCP-15).

`create_dynamic` set allow_dynamic=True and then, when initial_count > 0,
delegated to `create_fixed` — which REPLACES parent.mi_config wholesale and
knows nothing about dynamic growth, so allow_dynamic reverted to its False
default. The documented WCP-15 example therefore produced a parent that refused
every subsequent AddMultiInstance at WARNING.

Both directions, plus the check that matters more than the flag:

  A  initial_count=0  -> allow_dynamic True   (the case that always worked)
  B  initial_count=2  -> allow_dynamic True   (the defect)
  C  the seeded instances are actually created (B must not be won by skipping them)
  D  AddMultiInstance is ACCEPTED, not merely permitted by a flag

D is the one that counts. A flag reading True proves nothing about whether the
handler honours it, and the handler is where the WARNING came from.

    python audit/evaluations/probe_dynamic_multi_instance.py
"""

from __future__ import annotations

import logging
import sys

logging.basicConfig(level=logging.CRITICAL)

from stabilize.models.stage import StageExecution  # noqa: E402
from stabilize.stages.multi_instance_builder import MultiInstanceBuilder  # noqa: E402


def _build(initial_count: int) -> tuple[StageExecution, list[StageExecution]]:
    parent = StageExecution.create(type="step", name="parent", ref_id="parent")
    stages = MultiInstanceBuilder.create_dynamic(
        parent_stage=parent,
        instance_type="step",
        initial_count=initial_count,
    )
    return parent, stages


def _drive_add(initial_count: int) -> tuple[bool, str]:
    import tempfile
    from datetime import timedelta

    from stabilize import Orchestrator, QueueProcessor, SqliteQueue, SqliteWorkflowStore, TaskRegistry, Workflow
    from stabilize.queue.messages import AddMultiInstance

    with tempfile.TemporaryDirectory() as d:
        url = f"sqlite:///{d}/mi.db"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, table_name="queue_messages")
        queue._create_table()
        parent, stages = _build(initial_count)
        wf = Workflow.create(application="probe", name="mi", stages=[parent, *[s for s in stages if s is not parent]])
        store.store(wf)
        Orchestrator(queue, store=store).start(wf)
        proc = QueueProcessor(queue, store=store, task_registry=TaskRegistry())
        proc.config.retry_delay = timedelta(milliseconds=50)
        queue.push(AddMultiInstance(execution_type=wf.type.value, execution_id=wf.id, stage_id=parent.id))
        for _ in range(40):
            if not proc.process_one():
                break
        refs = sorted(s.ref_id for s in store.retrieve(wf.id).stages if "_instance_" in s.ref_id)
        queue.close()
        store.close()
    want = f"parent_instance_{initial_count + 1}"
    return want in refs, f"instance stages after AddMultiInstance: {refs} (want {want} among them)"


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    print("=== A. initial_count=0 — the case that always worked (control) ===")
    print("    without this, a builder that always set True would look fixed")
    p0, s0 = _build(0)
    print(f"    allow_dynamic={p0.mi_config.allow_dynamic}  count={p0.mi_config.count}  stages={len(s0)}")
    results.append(("initial_count=0 allows dynamic", p0.mi_config.allow_dynamic is True, str(p0.mi_config.allow_dynamic)))

    print()
    print("=== B. initial_count=2 — the defect ===")
    p2, s2 = _build(2)
    print(f"    allow_dynamic={p2.mi_config.allow_dynamic}  count={p2.mi_config.count}  stages={len(s2)}")
    results.append(("initial_count>0 allows dynamic", p2.mi_config.allow_dynamic is True, str(p2.mi_config.allow_dynamic)))

    print()
    print("=== C. THE SEEDED INSTANCES STILL EXIST ===")
    print("    B must not be won by declining to create them")
    instances = [s for s in s2 if "_instance_" in s.ref_id]
    print(f"    instance stages created: {[s.ref_id for s in instances]}")
    results.append(("seeded instances are created", len(instances) == 2, f"{len(instances)} instances"))
    results.append(("count reflects the seed", p2.mi_config.count == 2, str(p2.mi_config.count)))

    print()
    print("=== D. AddMultiInstance IS ACCEPTED, not merely permitted by a flag ===")
    print("    drive the handler through a processor and look for the instance row")
    added, detail = _drive_add(initial_count=2)
    print(f"    {detail}")
    results.append(("a seeded parent accepts AddMultiInstance (instance row created)", added, detail))

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
    print(f"VERDICT: PASS — a seeded dynamic parent still accepts new instances ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
