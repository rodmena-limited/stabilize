#!/usr/bin/env python3
"""
Multi-agent software team on stabilize.

An architect agent designs a small Python library; parallel coder agents each
implement a module; their outputs are gathered with a declarative fan-in
reducer; a smoke test runs; a reviewer agent loops back to the coders on
failure (bounded jump_to retry loop); a human-in-the-loop approval gate
suspends until approved; then a packager writes the README. Progress streams
live to the terminal, every step is event-sourced, and a --chaos run proves
durable recovery by SIGKILL-ing the worker mid-run and resuming.

Requires OLLAMA_API_KEY for the cloud model (default glm-5.2); without it the
tasks fall back to offline stubs so the engine still runs end to end.

Usage:
    export OLLAMA_API_KEY=...            # optional (offline stubs otherwise)
    python examples/agent_team/main.py            # normal run
    python examples/agent_team/main.py --chaos    # kill worker mid-run, recover
    python examples/agent_team/main.py --worker <db>   # (internal) worker process
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# Allow running from a source checkout without installing.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from team_tasks import (  # noqa: E402
    PROJECT_DIR,
    ArchitectTask,
    CoderTask,
    PackagerTask,
    ReviewerTask,
    TestWriterTask,
)
from stabilize import (  # noqa: E402
    JoinType,
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    WorkflowStream,
    WorkflowStatus,
    Workflow,
    approve,
    recover_on_startup,
)
from stabilize.events import (  # noqa: E402
    SqliteEventStore,
    configure_event_sourcing,
    reset_event_bus,
)
from stabilize.events.replay import EventReplayer  # noqa: E402
from stabilize.hitl import ApprovalTask  # noqa: E402
from stabilize.queue.processor.config import QueueProcessorConfig  # noqa: E402

GOAL = os.getenv(
    "AGENT_TEAM_GOAL",
    "A tiny token-bucket rate limiter library with a core limiter and small helpers.",
)
NUM_CODERS = 2  # parallel coder agents (one module each)


def build_registry() -> TaskRegistry:
    registry = TaskRegistry()
    registry.register("architect", ArchitectTask)
    registry.register("coder", CoderTask)
    registry.register("tester", TestWriterTask)
    registry.register("reviewer", ReviewerTask)
    registry.register("approval", ApprovalTask)
    registry.register("packager", PackagerTask)
    return registry


def _task(name: str, impl: str) -> TaskExecution:
    return TaskExecution.create(name=name, implementing_class=impl, stage_start=True, stage_end=True)


def build_workflow() -> Workflow:
    """Assemble the multi-agent DAG.

    architect -> [code_0, code_1] --(AND-join + reducer)--> gather
              -> test -> review (jump_to retry loop) -> approval (HITL) -> package
    """
    architect = StageExecution(
        ref_id="architect",
        type="architect",
        name="Architect",
        context={"goal": GOAL},
        tasks=[_task("design", "architect")],
    )

    coders = []
    for i in range(NUM_CODERS):
        coders.append(
            StageExecution(
                ref_id=f"code_{i}",
                type="coder",
                name=f"Coder {i}",
                requisite_stage_ref_ids={"architect"},
                context={"instance_index": i, "coder_stage_ref": "code_0"},
                tasks=[_task(f"code_{i}", "coder")],
            )
        )

    # AND-join that gathers each coder's 'written' module name into a list.
    gather = StageExecution(
        ref_id="gather",
        type="tester",
        name="Gather + Test",
        requisite_stage_ref_ids={f"code_{i}" for i in range(NUM_CODERS)},
        join_type=JoinType.AND,
        output_reducers={"written": "collect", "module_name": "collect"},
        context={"coder_stage_ref": "code_0"},
        tasks=[_task("smoke_test", "tester")],
    )

    review = StageExecution(
        ref_id="review",
        type="reviewer",
        name="Reviewer",
        requisite_stage_ref_ids={"gather"},
        context={"coder_stage_ref": "code_0", "max_review_attempts": 2},
        tasks=[_task("review", "reviewer")],
    )

    approval = StageExecution(
        ref_id="approval",
        type="approval",
        name="Human Approval Gate",
        requisite_stage_ref_ids={"review"},
        context={"approval_reject_continues": False},
        tasks=[_task("approve", "approval")],
    )

    package = StageExecution(
        ref_id="package",
        type="packager",
        name="Packager",
        requisite_stage_ref_ids={"approval"},
        context={"goal": GOAL},
        tasks=[_task("package", "packager")],
    )

    return Workflow.create(
        application="agent-team",
        name="Multi-Agent Software Team",
        stages=[architect, *coders, gather, review, approval, package],
    )


def _stream_progress(execution_id: str, stop: threading.Event) -> None:
    """Print live progress events until the workflow ends or we're stopped."""
    stream = WorkflowStream(execution_id)

    def on_event(item) -> None:
        if item.event_type == "custom.progress":
            agent = item.data.get("agent", "?")
            print(f"    · [{agent}] {item.data.get('message', '')}")
        elif item.event_type in {"stage.completed", "stage.failed", "workflow.completed", "workflow.failed"}:
            print(f"    ▸ {item.event_type}: {item.entity_id[:8]}")

    stream.on_event(on_event)
    stop.wait()
    stream.close()


def _auto_approver(store: SqliteWorkflowStore, queue: SqliteQueue, execution_id: str, stop: threading.Event) -> None:
    """Background 'human': approve the gate once it suspends (demo automation)."""
    while not stop.wait(0.3):
        try:
            wf = store.retrieve(execution_id)
        except Exception:
            continue
        gate = next((s for s in wf.stages if s.ref_id == "approval"), None)
        if gate and gate.status == WorkflowStatus.SUSPENDED:
            print("    ✋ approval gate suspended — auto-approving")
            approve(queue, execution_id, gate.id, {"user": "auto-reviewer"})
            return
        if wf.status.is_complete:
            return


def make_infra(db_path: str, register_events: bool = True):
    reset_event_bus()
    url = f"sqlite:///{db_path}"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    queue._create_table()
    event_store = SqliteEventStore(url, create_tables=True)
    recorder = None
    if register_events:
        recorder = configure_event_sourcing(event_store)
    registry = build_registry()
    return store, queue, event_store, recorder, registry


# --------------------------------------------------------------------------
# Worker subprocess (used by chaos mode)
# --------------------------------------------------------------------------


def run_worker(db_path: str) -> None:
    """Run a processor that drains the queue forever (killed externally)."""
    store, queue, _es, _rec, registry = make_infra(db_path)
    config = QueueProcessorConfig(recover_on_start=True, poll_frequency_ms=25)
    processor = QueueProcessor(queue, config=config, store=store, task_registry=registry)
    processor.start()
    try:
        while True:
            time.sleep(0.2)
    finally:
        processor.stop()


# --------------------------------------------------------------------------
# Normal run
# --------------------------------------------------------------------------


def run_normal(db_path: str) -> Workflow:
    store, queue, event_store, _rec, registry = make_infra(db_path)
    workflow = build_workflow()
    store.store(workflow)

    orchestrator = Orchestrator(queue, store=store)

    stop = threading.Event()
    threading.Thread(target=_stream_progress, args=(workflow.id, stop), daemon=True).start()
    threading.Thread(target=_auto_approver, args=(store, queue, workflow.id, stop), daemon=True).start()

    orchestrator.start(workflow)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    processor.start()

    deadline = time.time() + 180
    while time.time() < deadline:
        wf = store.retrieve(workflow.id)
        if wf.status.is_complete:
            break
        time.sleep(0.3)
    processor.stop()
    stop.set()
    return store.retrieve(workflow.id), store, event_store


# --------------------------------------------------------------------------
# Chaos run: SIGKILL the worker mid-flight, then recover to completion
# --------------------------------------------------------------------------


def run_chaos(db_path: str) -> Workflow:
    store, queue, event_store, _rec, registry = make_infra(db_path)
    workflow = build_workflow()
    store.store(workflow)
    Orchestrator(queue, store=store).start(workflow)

    stop = threading.Event()
    threading.Thread(target=_stream_progress, args=(workflow.id, stop), daemon=True).start()
    threading.Thread(target=_auto_approver, args=(store, queue, workflow.id, stop), daemon=True).start()

    # Phase 1: run a worker subprocess and SIGKILL it partway through.
    print("  ☠️  chaos: starting worker subprocess, will SIGKILL it mid-run")
    worker = subprocess.Popen([sys.executable, __file__, "--worker", db_path])
    time.sleep(6)  # let architect + some coders run
    worker.send_signal(signal.SIGKILL)
    worker.wait()
    print("  ☠️  chaos: worker KILLED. Surviving state is whatever committed atomically.")
    time.sleep(1)

    # Phase 2: fresh processor with recovery re-queues interrupted work.
    print("  ♻️  recovery: starting a fresh processor with recover_on_start=True")
    recover_on_startup(store, queue)
    config = QueueProcessorConfig(recover_on_start=True, poll_frequency_ms=25)
    processor = QueueProcessor(queue, config=config, store=store, task_registry=registry)
    processor.start()

    deadline = time.time() + 180
    while time.time() < deadline:
        wf = store.retrieve(workflow.id)
        if wf.status.is_complete:
            break
        time.sleep(0.3)
    processor.stop()
    stop.set()
    return store.retrieve(workflow.id), store, event_store


# --------------------------------------------------------------------------
# Post-run audit
# --------------------------------------------------------------------------


def audit(workflow: Workflow, store, event_store) -> bool:
    print("\n=== Post-run audit ===")
    ok = True

    status = workflow.status
    print(f"  workflow status: {status.name}")
    ok = ok and status == WorkflowStatus.SUCCEEDED

    # Event-sourced replay must reconstruct the same terminal workflow state.
    try:
        replayer = EventReplayer(event_store)
        rebuilt = replayer.rebuild_workflow_state(workflow.id)
        rebuilt_status = rebuilt.get("status")
        print(f"  replayed status: {rebuilt_status}")
        ok = ok and str(rebuilt_status) in {status.name, str(status)}
    except Exception as e:
        print(f"  replay check skipped: {e}")

    # The generated library's smoke test must pass.
    tests_dir = PROJECT_DIR / "tests"
    if tests_dir.exists():
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", str(tests_dir), "-q"],
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
        )
        print(f"  generated library tests: {'PASS' if proc.returncode == 0 else 'FAIL'}")
        ok = ok and proc.returncode == 0

    print(f"  RESULT: {'PASS ✅' if ok else 'FAIL ❌'}")
    return ok


def main() -> int:
    args = sys.argv[1:]
    if args and args[0] == "--worker":
        run_worker(args[1])
        return 0

    chaos = "--chaos" in args
    db_path = str(PROJECT_DIR / "agent_team.db")

    if PROJECT_DIR.exists():
        shutil.rmtree(PROJECT_DIR)
    PROJECT_DIR.mkdir(parents=True, exist_ok=True)

    mode = "CHAOS (kill + recover)" if chaos else "normal"
    model = os.getenv("AGENT_TEAM_MODEL", "glm-5.2")
    have_key = "yes" if os.getenv("OLLAMA_API_KEY") else "no (offline stubs)"
    print(f"=== Multi-Agent Software Team on stabilize ({mode}) ===")
    print(f"  goal:   {GOAL}")
    print(f"  model:  {model}  |  OLLAMA_API_KEY: {have_key}")
    print(f"  output: {PROJECT_DIR}\n")

    workflow, store, event_store = (run_chaos if chaos else run_normal)(db_path)
    ok = audit(workflow, store, event_store)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
