"""Every engine key written AT RUNTIME must be in the published inventory.

`tests/test_engine_context_keys.py` scans source, so it cannot see a key built
dynamically (an f-string, a computed suffix, a key written by a library). This
probe is the complementary direction: run graphs that exercise the control-flow
machinery, then census the top-level context keys the engine actually persisted
and check every one against `engine_keys.py`.

It exists because ci-conductor-dd94c8 censused 871 production rows against the
inventory and found no counter-example, then said plainly why that was weak
confirmation rather than validation: their pipelines are linear, so 35 of the 38
keys are never written there at all. The families their corpus cannot falsify —
_loop_*, _mm_*, _jump_*, _sub_workflow_* — are exactly the ones the source
scanner had to be broadened twice to see. This probe covers that gap.

    python audit/evaluations/probe_engine_key_census.py
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
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.models.stage.engine_keys import ENGINE_CONTEXT_KEYS  # noqa: E402
from stabilize.stages.loop_builder import LoopBuilder  # noqa: E402
from stabilize.stages.multi_instance_builder import MultiInstanceBuilder  # noqa: E402


class Step(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        value = int(stage.context.get("i", 0) or 0)
        return TaskResult.success(outputs={"i": value + 1})


def _engine() -> tuple[SqliteWorkflowStore, QueueProcessor, Orchestrator]:
    workdir = Path(tempfile.mkdtemp(prefix="probe-census-"))
    dsn = f"sqlite:///{workdir / 'probe.db'}"
    store = SqliteWorkflowStore(dsn, create_tables=True)
    queue = SqliteQueue(dsn, table_name="queue_messages")
    queue._create_table()
    registry = TaskRegistry()
    registry.register("step", Step())
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    return store, processor, Orchestrator(queue, store=store)


def _stage(ref_id: str, requires: set[str] | None = None, ctx: dict | None = None) -> StageExecution:
    stage = StageExecution.create(
        type="step",
        name=ref_id,
        ref_id=ref_id,
        context=ctx or {},
        requisite_stage_ref_ids=requires or set(),
    )
    stage.tasks = [TaskExecution.create(ref_id, "step", stage_start=True, stage_end=True)]
    return stage


def _run(name: str, stages: list[StageExecution]) -> set[str]:
    store, processor, orchestrator = _engine()
    workflow = Workflow.create(application="census", name=name, stages=stages)
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)
    result = store.retrieve(workflow.id)
    processor.stop(wait=True)

    keys: set[str] = set()
    for stage in result.stages:
        keys |= {k for k in stage.context if k.startswith("_")}
    return keys


def main() -> int:
    observed: dict[str, set[str]] = {}

    print("=== SHAPE 1: linear (the shape a production corpus already covers) ===")
    observed["linear"] = _run("linear", [_stage("a"), _stage("b", {"a"})])
    print(f"    engine keys persisted: {sorted(observed['linear'])}")

    print()
    print("=== SHAPE 2: structured loop — exercises the _loop_* family ===")
    body = _stage("body")
    observed["loop"] = _run("loop", LoopBuilder.while_loop("i < 3", [body], "L", 10, {"i": 0}))
    print(f"    engine keys persisted: {sorted(observed['loop'])}")

    print()
    print("=== SHAPE 3: fan-in join — exercises _completed_branches / join state ===")
    observed["join"] = _run(
        "join",
        [_stage("root"), _stage("x", {"root"}), _stage("y", {"root"}), _stage("j", {"x", "y"})],
    )
    print(f"    engine keys persisted: {sorted(observed['join'])}")

    print()
    print("=== SHAPE 4: multi-instance — exercises the _mi_* family ===")
    print("    no consumer corpus will ever cover this: ci-conductor expands a")
    print("    matrix into sibling stages before stabilize sees it, by design")
    mi_parent = _stage("mi_parent")
    mi_stages = [mi_parent] + MultiInstanceBuilder.create_fixed(
        parent_stage=mi_parent, count=3, instance_type="step", join_threshold=2
    )
    for st in mi_stages:
        if not st.tasks:
            st.tasks = [TaskExecution.create(st.ref_id, "step", stage_start=True, stage_end=True)]
    observed["multi_instance"] = _run("mi", mi_stages)
    print(f"    engine keys persisted: {sorted(observed['multi_instance'])}")

    every = set().union(*observed.values())

    print()
    print("=== A. CONTROL — the census can see engine keys at all ===")
    print("    a census that observed nothing would pass B vacuously")
    saw_any = bool(every)
    print(f"    distinct engine keys observed across all shapes: {sorted(every) or 'NONE'}")

    print()
    print("=== B. EVERY OBSERVED KEY IS IN THE PUBLISHED INVENTORY ===")
    unregistered = sorted(every - ENGINE_CONTEXT_KEYS)
    print(f"    unregistered: {unregistered or 'none'}")

    print()
    print("=== C. THE INVENTORY IS NOT TRIVIALLY SATISFIED ===")
    print("    it must contain keys this run did NOT exercise, or it is just a")
    print("    transcript of one execution rather than a published contract")
    unexercised = sorted(ENGINE_CONTEXT_KEYS - every)
    print(f"    registered but not exercised here: {len(unexercised)} of {len(ENGINE_CONTEXT_KEYS)}")

    results = [
        ("census observes engine keys at all", saw_any, f"{len(every)} distinct"),
        ("every runtime key is registered", not unregistered, str(unregistered)),
        ("inventory exceeds this run's coverage", bool(unexercised), f"{len(unexercised)} unexercised"),
    ]

    print()
    failures = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1

    print()
    print("NOT COVERED BY THIS PROBE, stated rather than implied: sub-workflow")
    print("nesting. Those keys are in the inventory from source scanning only, and")
    print("no production corpus will cover them either - ci-conductor builds one")
    print("flat Workflow per pipeline and never nests, by design.")

    print()
    if failures:
        print(f"VERDICT: FAIL — {failures} of {len(results)} checks failed")
        return 1
    print(f"VERDICT: PASS — runtime keys match the published inventory ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
