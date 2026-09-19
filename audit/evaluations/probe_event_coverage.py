"""Audit probe (#30): the event log must record what the engine actually did.

Seven recorder methods ship with replay branches and zero call sites, and there
is no event type for suspension at all. The consequences are operational:

  FINDING A  a human-approval wait is an unexplained silence. Nothing marks the
             stage as suspended, so replay shows it RUNNING and an operator
             cannot tell "waiting on a person" from "stuck".

  FINDING B  a jump (loop-back, retry-from, restart) leaves no trace, so a
             workflow that looped forty times replays as though it ran once.

  FINDING C  a task retry is not recorded, so a retry storm is invisible.

  CONTROL    the events that ARE wired (workflow/stage/task lifecycle) must keep
             being recorded. Without this the probe could pass by breaking event
             recording altogether.

Exit 0 = the engine records suspension, jumps and retries.

Run:  python audit/evaluations/probe_event_coverage.py
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
from stabilize.errors import TransientError  # noqa: E402
from stabilize.events import configure_event_sourcing, reset_event_recorder  # noqa: E402
from stabilize.events.store.sqlite import SqliteEventStore  # noqa: E402
from stabilize.hitl import ApprovalTask, approve  # noqa: E402


class Flaky(Task):
    """Fails once with a transient error, then succeeds."""

    attempts = 0

    def execute(self, stage: StageExecution) -> TaskResult:
        Flaky.attempts += 1
        if Flaky.attempts == 1:
            raise TransientError("first attempt fails")
        return TaskResult.success(outputs={"ok": True})


class JumpOnce(Task):
    """Jumps back to the head stage exactly once."""

    jumped = False

    def execute(self, stage: StageExecution) -> TaskResult:
        if not JumpOnce.jumped:
            JumpOnce.jumped = True
            return TaskResult.jump_to("head")
        return TaskResult.success()


def _stage(ref: str, impl: str, reqs: set[str] | None = None) -> StageExecution:
    return StageExecution(
        ref_id=ref,
        type=impl,
        name=ref,
        requisite_stage_ref_ids=reqs or set(),
        tasks=[TaskExecution.create(ref, impl, stage_start=True, stage_end=True)],
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        url = f"sqlite:///{Path(tmp) / 'p.db'}"
        event_store = SqliteEventStore(f"sqlite:///{Path(tmp) / 'events.db'}")
        reset_event_recorder()
        configure_event_sourcing(event_store, publish_to_bus=False)

        store = SqliteWorkflowStore(url, create_tables=True)
        queue = SqliteQueue(url, table_name="queue_messages")
        queue._create_table()
        registry = TaskRegistry()
        registry.register("approval", ApprovalTask)
        registry.register("flaky", Flaky)
        registry.register("jumponce", JumpOnce)
        processor = QueueProcessor(queue, store=store, task_registry=registry)
        runner = Orchestrator(queue, store=store)

        # A workflow that retries, jumps, and then waits on a human.
        workflow = Workflow.create(
            application="probe-30",
            name="coverage",
            stages=[
                _stage("head", "flaky"),
                _stage("looper", "jumponce", {"head"}),
                _stage("gate", "approval", {"looper"}),
            ],
        )
        store.store(workflow)
        runner.start(workflow)
        processor.process_all(timeout=30.0)

        gate = next(s for s in store.retrieve(workflow.id).stages if s.ref_id == "gate")
        suspended_types = {e.event_type.value for e in event_store.get_events_for_workflow(workflow.id)}
        print(f"gate status while waiting: {gate.status.name}")
        print(f"event types recorded: {sorted(suspended_types)}")

        approve(queue, execution_id=workflow.id, stage_id=gate.id, data={"by": "alice"}, user="alice")
        processor.process_all(timeout=30.0)
        processor.stop(wait=True)

        events = event_store.get_events_for_workflow(workflow.id)
        types = {e.event_type.value for e in events}
        actors = {e.metadata.actor for e in events}
        print(f"final event types: {sorted(types)}")
        print(f"actors seen: {sorted(actors)}")
        print(f"retry attempts made: {Flaky.attempts}, jump taken: {JumpOnce.jumped}")
        print()

        # Control: the wired lifecycle events must still be there.
        if not {"workflow.created", "workflow.started"} & types:
            print("FAIL (control): lifecycle events are no longer recorded at all.")
            return 1

        missing = []
        if not any("suspend" in t for t in types):
            missing.append("suspension (FINDING A)")
        if "jump.executed" not in types:
            missing.append("jump.executed (FINDING B)")
        if "task.retried" not in types:
            missing.append("task.retried (FINDING C)")

        if missing:
            print(f"FAIL (#30): the event log never records: {', '.join(missing)}")
            return 1
        if actors == {"system"}:
            print("FAIL (#30): every event is attributed to 'system'; the approver is unrecorded.")
            return 1

        print("PASS: suspension, jumps and retries are recorded, with real actors.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
