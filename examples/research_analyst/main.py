#!/usr/bin/env python3
"""
Autonomous Research Analyst — a complex agentic workflow on stabilize.

A planner decomposes a research question; three ReAct tool-calling agents
research the sub-questions in parallel (real tool use over a calculator +
knowledge base); an N-of-M join gathers their findings via a declarative
reducer once a quorum finishes; a lead agent synthesizes; two adversarial
verifiers race and a DISCRIMINATOR join proceeds on the first verdict; a
router loops back to the planner to refine (a bounded, workflow-level agent
cycle); a human-in-the-loop gate approves; and a report SUB-WORKFLOW (a real
child DAG) writes the final report. Everything streams live and is
event-sourced, and a --chaos run SIGKILLs the worker mid-flight and recovers.

This exercises a superset of LangGraph's feature demos — see README.md for the
parity mapping.

Model: ollama.com cloud glm-5.2 via OLLAMA_API_KEY (offline stubs if unset).

Usage:
    export OLLAMA_API_KEY=...
    python examples/research_analyst/main.py            # normal run
    python examples/research_analyst/main.py --chaos    # kill worker + recover
    python examples/research_analyst/main.py --worker <db>   # (internal)
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

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from analyst_tasks import (  # noqa: E402
    NUM_RESEARCHERS,
    OUT_DIR,
    DraftReportTask,
    PlannerTask,
    PolishReportTask,
    ReportSubWorkflowTask,
    ResearchAgentTask,
    RouterTask,
    SynthesizerTask,
    VerifierTask,
)

from stabilize import (  # noqa: E402
    ApprovalTask,
    JoinType,
    Orchestrator,
    QueueProcessor,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
    WorkflowStatus,
    WorkflowStream,
    approve,
    recover_on_startup,
)
from stabilize.events import SqliteEventStore, configure_event_sourcing, reset_event_bus  # noqa: E402
from stabilize.events.replay import EventReplayer  # noqa: E402
from stabilize.queue.processor.config import QueueProcessorConfig  # noqa: E402

QUESTION = os.getenv(
    "ANALYST_QUESTION",
    "How many GPU servers do we need to serve the atlas-70b model at the target "
    "workload, and what is the cheapest option that meets it?",
)


def _task(name: str, impl: str) -> TaskExecution:
    return TaskExecution.create(name=name, implementing_class=impl, stage_start=True, stage_end=True)


def build_registry() -> TaskRegistry:
    registry = TaskRegistry()
    registry.register("planner", PlannerTask)
    registry.register("researcher", ResearchAgentTask)
    registry.register("synthesizer", SynthesizerTask)
    registry.register("verifier", VerifierTask)
    registry.register("router", RouterTask)
    registry.register("approval", ApprovalTask)
    registry.register("report_subworkflow", ReportSubWorkflowTask)
    # Child (sub-workflow) tasks share the same registry.
    registry.register("draft_report", DraftReportTask)
    registry.register("polish_report", PolishReportTask)
    return registry


def build_workflow() -> Workflow:
    """Assemble the analyst DAG.

    plan -> [research_0..2] --(N_OF_M >=2 + reducer)--> synthesize
         -> [verify_a, verify_b] --(DISCRIMINATOR)--> route (cycle back to plan)
         -> approve (HITL) -> report (sub-workflow)
    """
    plan = StageExecution(
        ref_id="plan",
        type="planner",
        name="Planner",
        context={
            "question": QUESTION,
            "min_passes": int(os.getenv("ANALYST_MIN_PASSES", "1")),
            "max_passes": int(os.getenv("ANALYST_MAX_PASSES", "3")),
        },
        tasks=[_task("plan", "planner")],
    )

    researchers = [
        StageExecution(
            ref_id=f"research_{i}",
            type="researcher",
            name=f"Researcher {i}",
            requisite_stage_ref_ids={"plan"},
            context={"instance_index": i, "continuePipelineOnFailure": True},
            tasks=[_task(f"research_{i}", "researcher")],
        )
        for i in range(NUM_RESEARCHERS)
    ]

    # N-of-M join: proceed once a quorum of researchers finishes; the reducer
    # gathers each branch's 'finding' into a list. (LangGraph has no native
    # "proceed on K of N".)
    synthesize = StageExecution(
        ref_id="synthesize",
        type="synthesizer",
        name="Synthesizer",
        requisite_stage_ref_ids={f"research_{i}" for i in range(NUM_RESEARCHERS)},
        join_type=JoinType.N_OF_M,
        join_threshold=2,
        output_reducers={"finding": "collect"},
        tasks=[_task("synthesize", "synthesizer")],
    )

    verify_a = StageExecution(
        ref_id="verify_a",
        type="verifier",
        name="Verifier (correctness)",
        requisite_stage_ref_ids={"synthesize"},
        context={"lens": "correctness"},
        tasks=[_task("verify_a", "verifier")],
    )
    verify_b = StageExecution(
        ref_id="verify_b",
        type="verifier",
        name="Verifier (cost)",
        requisite_stage_ref_ids={"synthesize"},
        context={"lens": "cost"},
        tasks=[_task("verify_b", "verifier")],
    )

    # Discriminator join: proceed on whichever verifier lands first.
    route = StageExecution(
        ref_id="route",
        type="router",
        name="Router",
        requisite_stage_ref_ids={"verify_a", "verify_b"},
        join_type=JoinType.DISCRIMINATOR,
        context={
            "confidence_threshold": float(os.getenv("ANALYST_CONF_THRESHOLD", "0.85")),
            "min_passes": int(os.getenv("ANALYST_MIN_PASSES", "1")),
            "max_passes": int(os.getenv("ANALYST_MAX_PASSES", "3")),
        },
        tasks=[_task("route", "router")],
    )

    approve_gate = StageExecution(
        ref_id="approve",
        type="approval",
        name="Human Approval Gate",
        requisite_stage_ref_ids={"route"},
        tasks=[_task("approve", "approval")],
    )

    report = StageExecution(
        ref_id="report",
        type="report_subworkflow",
        name="Report Sub-Workflow",
        requisite_stage_ref_ids={"approve"},
        # The child DAG is built at runtime by the task (see analyst_tasks);
        # it must not be passed through context, which is JSON-serialized.
        tasks=[_task("report", "report_subworkflow")],
    )

    return Workflow.create(
        application="research-analyst",
        name="Autonomous Research Analyst",
        stages=[plan, *researchers, synthesize, verify_a, verify_b, route, approve_gate, report],
    )


# --------------------------------------------------------------------------
# Live streaming + auto-approver
# --------------------------------------------------------------------------


def _stream(execution_id: str, stop: threading.Event) -> None:
    stream = WorkflowStream(execution_id)

    def on_event(item) -> None:
        if item.event_type == "custom.progress":
            agent = item.data.get("agent", "?")
            print(f"    · [{agent}] {item.data.get('message', '')}")
        elif item.event_type in {"stage.failed", "workflow.completed", "workflow.failed"}:
            print(f"    ▸ {item.event_type}")

    stream.on_event(on_event)
    stop.wait()
    stream.close()


def _auto_approver(store, queue, execution_id: str, stop: threading.Event) -> None:
    while not stop.wait(0.3):
        try:
            wf = store.retrieve(execution_id)
        except Exception:
            continue
        gate = next((s for s in wf.stages if s.ref_id == "approve"), None)
        if gate and gate.status == WorkflowStatus.SUSPENDED:
            print("    ✋ approval gate suspended — auto-approving")
            approve(queue, execution_id, gate.id, {"user": "auto-analyst"})
            return
        if wf.status.is_complete:
            return


def _configure_sqlite() -> None:
    """Use WAL for the demo so a SIGKILLed worker sharing the DB file with the
    parent recovers cleanly instead of corrupting the rollback journal."""
    os.environ.setdefault("STABILIZE_SQLITE_JOURNAL_MODE", "WAL")
    os.environ.setdefault("STABILIZE_SQLITE_SYNCHRONOUS", "NORMAL")
    os.environ.setdefault("STABILIZE_SQLITE_BUSY_TIMEOUT_MS", "30000")


def make_infra(db_path: str, register_events: bool = True):
    _configure_sqlite()
    reset_event_bus()
    url = f"sqlite:///{db_path}"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    queue._create_table()
    event_store = SqliteEventStore(url, create_tables=True)
    if register_events:
        configure_event_sourcing(event_store)
    registry = build_registry()
    return store, queue, event_store, registry


# --------------------------------------------------------------------------
# Runners
# --------------------------------------------------------------------------


def run_worker(db_path: str) -> None:
    store, queue, _es, registry = make_infra(db_path)
    config = QueueProcessorConfig(recover_on_start=True, poll_frequency_ms=25)
    processor = QueueProcessor(queue, config=config, store=store, task_registry=registry)
    processor.start()
    try:
        while True:
            time.sleep(0.2)
    finally:
        processor.stop()


def _drive(store, queue, registry, event_store, workflow, chaos: bool):
    orchestrator = Orchestrator(queue, store=store)
    store.store(workflow)
    stop = threading.Event()

    if chaos:
        # Phase 1: a worker SUBPROCESS owns execution; SIGKILL it mid-research.
        # The parent stays hands-off the DB during the kill window (its events
        # live in the subprocess anyway), so streaming/approval start in the
        # recovery phase where they are in-process and useful.
        orchestrator.start(workflow)
        print("  ☠️  chaos: worker subprocess will be SIGKILLed mid-research")
        worker = subprocess.Popen([sys.executable, __file__, "--worker", store.connection_string.split("///")[-1]])
        time.sleep(9)
        worker.send_signal(signal.SIGKILL)
        worker.wait()
        print("  ☠️  chaos: worker KILLED — recovering from durable state")
        time.sleep(1)
        recover_on_startup(store, queue)
        config = QueueProcessorConfig(recover_on_start=True, poll_frequency_ms=25)
        processor = QueueProcessor(queue, config=config, store=store, task_registry=registry)
    else:
        orchestrator.start(workflow)
        processor = QueueProcessor(queue, store=store, task_registry=registry)

    threading.Thread(target=_stream, args=(workflow.id, stop), daemon=True).start()
    threading.Thread(target=_auto_approver, args=(store, queue, workflow.id, stop), daemon=True).start()

    processor.start()
    deadline = time.time() + 360
    while time.time() < deadline:
        if store.retrieve(workflow.id).status.is_complete:
            break
        time.sleep(0.4)
    processor.stop()
    stop.set()
    return store.retrieve(workflow.id)


# --------------------------------------------------------------------------
# Audit
# --------------------------------------------------------------------------


def audit(workflow, store, event_store) -> bool:
    print("\n=== Post-run audit ===")
    ok = workflow.status == WorkflowStatus.SUCCEEDED
    print(f"  workflow status: {workflow.status.name}")

    # Report on which advanced features actually fired.
    stages = {s.ref_id: s for s in workflow.stages}
    plan_passes = stages["plan"].outputs.get("plan_pass", 0) if "plan" in stages else 0
    print(f"  planning passes (cycle): {plan_passes + 1}")
    completed_researchers = sum(
        1 for i in range(NUM_RESEARCHERS) if stages.get(f"research_{i}") and stages[f"research_{i}"].status.is_complete
    )
    print(f"  researchers completed: {completed_researchers}/{NUM_RESEARCHERS} (N-of-M quorum was 2)")
    if "route" in stages:
        print(f"  discriminator winner confidence: {stages['route'].context.get('confidence', 'n/a')}")

    # Event-sourced replay equivalence.
    try:
        rebuilt = EventReplayer(event_store).rebuild_workflow_state(workflow.id)
        match = str(rebuilt.get("status")) in {workflow.status.name, str(workflow.status)}
        print(f"  event-sourced replay status: {rebuilt.get('status')} ({'match' if match else 'MISMATCH'})")
        ok = ok and match
    except Exception as e:
        print(f"  replay check skipped: {e}")

    report_path = OUT_DIR / "report.md"
    if report_path.exists():
        print(f"  report written: {report_path} ({len(report_path.read_text())} bytes)")
    else:
        print("  report: MISSING")
        ok = False

    print(f"  RESULT: {'PASS ✅' if ok else 'FAIL ❌'}")
    return ok


def main() -> int:
    args = sys.argv[1:]
    if args and args[0] == "--worker":
        run_worker(args[1])
        return 0

    chaos = "--chaos" in args
    db_path = str(OUT_DIR / "analyst.db")
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    have_key = "yes" if os.getenv("OLLAMA_API_KEY") else "no (offline stubs)"
    print(f"=== Autonomous Research Analyst ({'CHAOS' if chaos else 'normal'}) ===")
    print(f"  question: {QUESTION}")
    print(f"  model:    {os.getenv('ANALYST_MODEL', 'glm-5.2')}  |  OLLAMA_API_KEY: {have_key}\n")

    store, queue, event_store, registry = make_infra(db_path)
    workflow = build_workflow()
    result = _drive(store, queue, registry, event_store, workflow, chaos)
    return 0 if audit(result, store, event_store) else 1


if __name__ == "__main__":
    raise SystemExit(main())
