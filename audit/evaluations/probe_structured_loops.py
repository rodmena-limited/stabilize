"""LoopBuilder emits stages naming task classes that must actually exist.

LoopBuilder named LoopConditionTask and LoopBackTask as implementing classes
while neither was registered anywhere, so every structured loop died at its
first condition check with TaskNotFoundError.

Three directions in one run: a loop that must iterate exactly N times, a loop
whose condition is false from the start and must not execute its body at all,
and a control proving a caller can still take those names for themselves.

    python audit/evaluations/probe_structured_loops.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.ERROR)

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
from stabilize.stages.loop_builder import LoopBuilder  # noqa: E402

BUILTIN_LOOP_TASKS = ("LoopConditionTask", "LoopBackTask", "LoopEntryTask")


class Counter(Task):
    def __init__(self) -> None:
        self.seen: list[int] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        value = int(stage.context.get("i", 0) or 0)
        self.seen.append(value)
        return TaskResult.success(outputs={"i": value + 1})


def _body() -> StageExecution:
    return StageExecution(
        ref_id="body",
        type="body",
        name="body",
        tasks=[TaskExecution.create("body", "body", stage_start=True, stage_end=True)],
    )


def _run(stages: list[StageExecution], name: str) -> tuple[Workflow, Counter]:
    workdir = Path(tempfile.mkdtemp(prefix="probe-loops-"))
    dsn = f"sqlite:///{workdir / 'probe.db'}"
    store = SqliteWorkflowStore(dsn, create_tables=True)
    queue = SqliteQueue(dsn, table_name="queue_messages")
    queue._create_table()

    counter = Counter()
    registry = TaskRegistry()
    registry.register("body", counter)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    orchestrator = Orchestrator(queue, store=store)

    workflow = Workflow.create(application="probe-loops", name=name, stages=stages)
    store.store(workflow)
    orchestrator.start(workflow)
    processor.process_all(timeout=120.0)
    result = store.retrieve(workflow.id)
    processor.stop(wait=True)
    return result, counter


def main() -> int:
    failures: list[str] = []

    print("=== A. THE BUILT-IN LOOP TASKS EXIST (this is what #31 was) ===")
    registry = TaskRegistry()
    for name in BUILTIN_LOOP_TASKS:
        present = registry.has(name)
        print(f"    {name:20} registered: {present}")
        if not present:
            failures.append(f"{name} is not registered, so any loop naming it cannot run")

    print()
    print("=== B. CONTROL — a caller can still take one of those names ===")
    print("    without this, 'registered' could mean the names were simply reserved")

    class Mine(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            return TaskResult.success()

    own = TaskRegistry()
    mine = Mine()
    own.register("LoopConditionTask", mine)
    took_it = own.get("LoopConditionTask") is mine
    print(f"    caller override honoured: {took_it}")
    if not took_it:
        failures.append("seeding the built-ins took a name away from the caller")

    print()
    print("=== C. A WHILE LOOP RUNS ITS BODY EXACTLY N TIMES ===")
    result, counter = _run(
        LoopBuilder.while_loop("i < 3", [_body()], "L", 10, {"i": 0}), "while"
    )
    print(f"    body saw iterations: {counter.seen}")
    print(f"    workflow status:     {result.status}")
    if counter.seen != [0, 1, 2]:
        failures.append(f"expected body to see [0, 1, 2], saw {counter.seen}")
    if result.status != WorkflowStatus.SUCCEEDED:
        failures.append(f"while-loop workflow ended {result.status}, expected SUCCEEDED")

    print()
    print("=== D. A FALSE CONDITION NEVER EXECUTES THE BODY ===")
    print("    the other direction: C alone cannot distinguish 'loops work'")
    print("    from 'the body runs regardless of the condition'")
    result_zero, counter_zero = _run(
        LoopBuilder.while_loop("i < 0", [_body()], "Z", 10, {"i": 0}), "zero"
    )
    print(f"    body saw iterations: {counter_zero.seen}")
    print(f"    workflow status:     {result_zero.status}")
    if counter_zero.seen != []:
        failures.append(f"body ran on a false condition: saw {counter_zero.seen}")
    if result_zero.status != WorkflowStatus.SUCCEEDED:
        failures.append(f"false-condition workflow ended {result_zero.status}")

    print()
    if failures:
        print("FAIL: structured loops are not working as specified")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("PASS: structured loops execute, honour their condition, and do not")
    print("      take task names away from the caller.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
