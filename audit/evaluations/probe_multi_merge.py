"""Audit probe (#34): MULTI_MERGE must fire once per upstream completion.

JoinType.MULTI_MERGE is documented as "fire once per upstream completion, no
synchronisation" (WCP-8). It was implemented as a readiness predicate that
returns READY whenever any upstream is complete, with the firing bookkeeping
delegated to a caller branch that was never written -- so the 2nd..Nth triggers
are swallowed and the stage behaves as an AND-join that ignores its parents.

  FINDING    a MULTI_MERGE stage with N upstreams executes once, not N times.

  CONTROL 1  the same graph with a plain AND-join must still execute ONCE.
             Without this the probe could pass by making everything re-fire.

  CONTROL 2  the merge stage's own downstream must run ONCE. Multiplicity does
             not propagate in this model, and that limit is asserted on purpose
             rather than left implied.

Exit 0 = the merge fires per upstream, and neither control has moved.

Run:  python audit/evaluations/probe_multi_merge.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
from collections import Counter
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
from stabilize.models.stage import JoinType  # noqa: E402

RUNS: Counter[str] = Counter()


class Mark(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        RUNS[stage.ref_id] += 1
        return TaskResult.success(outputs={"n": RUNS[stage.ref_id]})


def _stage(ref: str, reqs: set[str] | None = None, **kw) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type="mark",
        name=ref,
        requisite_stage_ref_ids=reqs or set(),
        tasks=[TaskExecution.create(ref, "mark", stage_start=True, stage_end=True)],
        **kw,
    )


def _run(name: str, join: JoinType) -> Workflow:
    RUNS.clear()
    with tempfile.TemporaryDirectory() as tmp:
        url = f"sqlite:///{Path(tmp) / 'p.db'}"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, table_name="queue_messages")
        queue._create_table()
        registry = TaskRegistry()
        registry.register("mark", Mark)
        processor = QueueProcessor(queue, store=store, task_registry=registry)
        runner = Orchestrator(queue, store=store)

        workflow = Workflow.create(
            application="probe-34",
            name=name,
            stages=[
                _stage("root"),
                _stage("a", {"root"}),
                _stage("b", {"root"}),
                _stage("c", {"root"}),
                _stage("mm", {"a", "b", "c"}, join_type=join),
                _stage("down", {"mm"}),
            ],
        )
        store.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=60.0)
        processor.stop(wait=True)
        return store.retrieve(workflow.id)


def main() -> int:
    result = _run("multi-merge", JoinType.MULTI_MERGE)
    mm_runs, down_runs = RUNS["mm"], RUNS["down"]
    mm_stage = next(s for s in result.stages if s.ref_id == "mm")
    consumed = mm_stage.context.get("_mm_consumed")
    print(f"MULTI_MERGE: mm ran {mm_runs}x (expect 3), down ran {down_runs}x (expect 1)")
    print(f"             _mm_consumed = {consumed}")
    print(f"             workflow = {result.status.name}")

    and_result = _run("and-join", JoinType.AND)
    and_mm = RUNS["mm"]
    print(f"AND control: mm ran {and_mm}x (expect 1), workflow = {and_result.status.name}")
    print()

    if and_mm != 1:
        print(f"FAIL (control 1): an AND-join fired {and_mm} times; the change over-fires.")
        return 1
    if and_result.status.name != "SUCCEEDED":
        print(f"FAIL (control 1): AND-join workflow ended {and_result.status.name}.")
        return 1
    if mm_runs != 3:
        print(f"FAIL (#34): MULTI_MERGE fired {mm_runs} times for 3 upstreams.")
        return 1
    if down_runs != 1:
        print(f"FAIL (control 2): downstream ran {down_runs} times; multiplicity leaked past the merge.")
        return 1
    if result.status.name != "SUCCEEDED":
        print(f"FAIL (#34): workflow ended {result.status.name}.")
        return 1

    print("PASS: the merge fires once per upstream; AND-join and downstream unchanged.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
