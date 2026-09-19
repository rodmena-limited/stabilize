"""Audit probe (#33): a de-selected branch must not execute, and a join with no
live branch must not fire.

An OR-split marks its non-activated children SKIPPED, but SKIPPED counts as
"upstream satisfied", so nothing stops the rest of that branch running.

  FINDING A  a branch deeper than one stage runs anyway. The documented XOR-split
             gate is therefore not a gate: a disabled production deploy still
             deploys.

  FINDING B  a join whose every upstream branch was de-selected still fires, so a
             merge point executes on a path no token ever reached.

  CONTROL    a diamond where one branch is de-selected and the other is live must
             still run the join exactly once. Without this the probe could pass by
             pruning everything.

Exit 0 = de-selected branches are pruned and live branches are untouched.

Run:  python audit/evaluations/probe_branch_pruning.py
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
from stabilize.models.stage import JoinType, SplitType  # noqa: E402

RAN: list[str] = []


class Mark(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        RAN.append(stage.ref_id)
        return TaskResult.success(outputs={"from": stage.ref_id})


def _stage(ref: str, reqs: set[str] | None = None, **kw) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type="mark",
        name=ref,
        requisite_stage_ref_ids=reqs or set(),
        tasks=[TaskExecution.create(ref, "mark", stage_start=True, stage_end=True)],
        **kw,
    )


def _run(name: str, stages: list[StageExecution]) -> Workflow:
    RAN.clear()
    with tempfile.TemporaryDirectory() as tmp:
        url = f"sqlite:///{Path(tmp) / 'p.db'}"
        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, table_name="queue_messages")
        queue._create_table()
        registry = TaskRegistry()
        registry.register("mark", Mark)
        processor = QueueProcessor(queue, store=store, task_registry=registry)
        runner = Orchestrator(queue, store=store)

        workflow = Workflow.create(application="probe-33", name=name, stages=stages)
        store.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=30.0)
        processor.stop(wait=True)
        return store.retrieve(workflow.id)


def main() -> int:
    failures = []

    # FINDING A: a two-deep de-selected branch.
    _run(
        "two-deep",
        [
            _stage("root", split_type=SplitType.OR,
                   split_conditions={"live": "True", "dead": "False"}),
            _stage("live", {"root"}),
            _stage("live2", {"live"}),
            _stage("dead", {"root"}),
            _stage("dead2", {"dead"}),
        ],
    )
    deep = sorted(RAN)
    print(f"two-deep split      -> ran {deep}")
    if "dead2" in deep:
        print("  FINDING A: a stage on the de-selected branch executed")
        failures.append("A")

    # FINDING B: a join whose every branch was de-selected.
    _run(
        "all-pruned-join",
        [
            _stage("root", split_type=SplitType.OR,
                   split_conditions={"live": "True", "b": "False", "c": "False"}),
            _stage("live", {"root"}),
            _stage("b", {"root"}),
            _stage("c", {"root"}),
            _stage("orjoin", {"b", "c"}, join_type=JoinType.OR),
        ],
    )
    joined = sorted(RAN)
    print(f"all-branches-pruned -> ran {joined}")
    if "orjoin" in joined:
        print("  FINDING B: a join fired with no live upstream branch")
        failures.append("B")

    # CONTROL: a diamond with one branch de-selected must still join, once.
    _run(
        "diamond",
        [
            _stage("root", split_type=SplitType.OR,
                   split_conditions={"left": "True", "right": "False"}),
            _stage("left", {"root"}),
            _stage("right", {"root"}),
            _stage("join", {"left", "right"}),
        ],
    )
    diamond = sorted(RAN)
    print(f"diamond, one live   -> ran {diamond}")
    if RAN.count("join") != 1:
        print(f"  FAIL (control): the join ran {RAN.count('join')} times, expected once")
        return 1

    print()
    if failures:
        print(f"FAIL (#33): de-selected branches are not pruned ({', '.join(failures)}).")
        return 1
    print("PASS: de-selected branches are pruned and live branches still join.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
