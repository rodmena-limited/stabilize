"""Audit probe (#27): a re-entered stage must observe fresh upstream outputs.

``_plan_stage`` merges ancestor outputs, lets the stage's own context override
them, then persists the merged result back onto the stage. The first value a
stage observes for a key is therefore frozen into its row and shadows every
later ancestor output.

This probe drives the documented WCP-10 ``jump_to`` retry pattern through the
public API. No LoopBuilder is involved.

  FINDING    the checker observes the same upstream value on every pass, so its
             exit condition can never become true; it exhausts the jump budget
             and the workflow fails.

  CONTROL    a key the caller sets directly on the stage context, which no
             ancestor publishes, must still win. Without this the probe could
             pass by making ancestors override everything.

Exit 0 = re-entry observes fresh values and caller context still wins.

Run:  python audit/evaluations/probe_stage_context_rehydration.py
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

TARGET = 3


class Worker(Task):
    """Publishes an incrementing attempt number as an output."""

    runs = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        Worker.runs += 1
        return TaskResult.success(outputs={"attempts": Worker.runs})


class Checker(Task):
    """Jumps back until it observes the upstream reaching TARGET."""

    seen: list[object] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        value = stage.context.get("attempts")
        Checker.seen.append(value)
        if isinstance(value, int) and value >= TARGET:
            return TaskResult.success(outputs={"converged": True})
        return TaskResult.jump_to("work")


class Keeper(Task):
    """Reports a caller-set key that no ancestor publishes."""

    observed: object = None

    def execute(self, stage: StageExecution) -> TaskResult:
        Keeper.observed = stage.context.get("caller_key")
        return TaskResult.success()


def _engine(db: Path):
    url = f"sqlite:///{db}"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url, table_name="queue_messages")
    queue._create_table()
    registry = TaskRegistry()
    registry.register("work", Worker)
    registry.register("check", Checker)
    registry.register("keep", Keeper)
    return store, queue, QueueProcessor(queue, store=store, task_registry=registry), Orchestrator(queue, store=store)


def _stage(ref: str, impl: str, reqs: set[str] | None = None, **kw) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type=impl,
        name=ref,
        requisite_stage_ref_ids=reqs or set(),
        tasks=[TaskExecution.create(ref, impl, stage_start=True, stage_end=True)],
        **kw,
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        store, queue, processor, runner = _engine(Path(tmp) / "c27.db")

        # Control runs as its OWN workflow: it must not depend on the retry
        # loop converging, or it cannot distinguish the defect from its effect.
        control = Workflow.create(
            application="probe-c27",
            name="caller-key",
            stages=[
                _stage("seed", "work"),
                _stage("keep", "keep", {"seed"}, context={"caller_key": "mine"}),
            ],
        )
        store.store(control)
        runner.start(control)
        processor.process_all(timeout=30.0)

        Worker.runs = 0
        workflow = Workflow.create(
            application="probe-c27",
            name="retry-loop",
            stages=[
                _stage("work", "work"),
                _stage("check", "check", {"work"}),
            ],
        )
        store.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=30.0)
        processor.stop(wait=True)

        result = store.retrieve(workflow.id)
        print(f"upstream produced attempts = {Worker.runs}")
        print(f"checker observed each pass = {Checker.seen}")
        print(f"caller-set key observed    = {Keeper.observed!r}")
        print(f"workflow                   = {result.status.name}")
        print()

        if Keeper.observed != "mine":
            print("FAIL (control): a caller-set context key was lost.")
            print("      The re-hydration check below cannot be trusted while this is false.")
            return 1

        distinct = sorted({v for v in Checker.seen if isinstance(v, int)})
        if distinct == [1] and len(Checker.seen) > 1:
            print("FAIL (#27): the re-entered stage observed the same upstream value every pass.")
            return 1
        if result.status.name != "SUCCEEDED":
            print(f"FAIL (#27): retry loop did not converge; workflow ended {result.status.name}.")
            return 1

        print("PASS: a re-entered stage observes fresh upstream outputs.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
