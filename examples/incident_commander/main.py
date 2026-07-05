#!/usr/bin/env python3
"""
Autonomous Incident Commander — a complex agentic workflow on stabilize.

An AI on-call commander works a production incident end to end:

  triage (tool-using agent)
    -> [diag: logs, diag: metrics, diag: deploys]   (3 parallel ReAct agents)
    -> root cause                                    (N-of-M quorum + fan-in reducer)
    -> human approval of the fix                     (durable HITL gate)
    -> remediate                                     (privileged action, changes real state)
    -> verify  --(still degraded? loop back)-->      (self-healing loop, bounded)
    -> post-mortem                                   (LLM writes the report)

Every step is durable and event-sourced; progress streams live like an incident
channel. Model: ollama.com cloud glm-5.2 via OLLAMA_API_KEY (offline stubs if
unset).

Usage:
    export OLLAMA_API_KEY=...
    python examples/incident_commander/main.py
"""

from __future__ import annotations

import os
import shutil
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from incident_tasks import (  # noqa: E402
    OUT_DIR,
    DiagnosticAgent,
    PostmortemTask,
    RemediateTask,
    RootCauseTask,
    TriageAgent,
    VerifyTask,
)
from tools import INCIDENT, reset_environment  # noqa: E402

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
)
from stabilize.events import SqliteEventStore, configure_event_sourcing, reset_event_bus  # noqa: E402
from stabilize.events.replay import EventReplayer  # noqa: E402

ALERT = os.getenv("INCIDENT_ALERT", "PagerDuty: checkout error rate > 5% for 10 minutes (p99 latency 3.4s)")
SERVICE = os.getenv("INCIDENT_SERVICE", "checkout")
NUM_DIAG = 3


def _task(name: str, impl: str) -> TaskExecution:
    return TaskExecution.create(name=name, implementing_class=impl, stage_start=True, stage_end=True)


def build_registry() -> TaskRegistry:
    registry = TaskRegistry()
    registry.register("triage", TriageAgent)
    registry.register("diagnose", DiagnosticAgent)
    registry.register("root_cause", RootCauseTask)
    registry.register("approval", ApprovalTask)
    registry.register("remediate", RemediateTask)
    registry.register("verify", VerifyTask)
    registry.register("postmortem", PostmortemTask)
    return registry


def build_workflow() -> Workflow:
    base = {"service": SERVICE, "alert": ALERT}

    triage = StageExecution(
        ref_id="triage", type="triage", name="Triage",
        context=dict(base), tasks=[_task("triage", "triage")],
    )
    diagnostics = [
        StageExecution(
            ref_id=f"diag_{i}", type="diagnose", name=f"Diagnose {i}",
            requisite_stage_ref_ids={"triage"},
            context={**base, "instance_index": i, "continuePipelineOnFailure": True},
            tasks=[_task(f"diag_{i}", "diagnose")],
        )
        for i in range(NUM_DIAG)
    ]
    root_cause = StageExecution(
        ref_id="root_cause", type="root_cause", name="Root Cause",
        requisite_stage_ref_ids={f"diag_{i}" for i in range(NUM_DIAG)},
        join_type=JoinType.N_OF_M, join_threshold=2,           # consensus on a quorum
        output_reducers={"finding": "collect"},                # gather each agent's finding
        context=dict(base), tasks=[_task("root_cause", "root_cause")],
    )
    approval = StageExecution(
        ref_id="approve", type="approval", name="Approve Remediation",
        requisite_stage_ref_ids={"root_cause"}, context=dict(base),
        tasks=[_task("approve", "approval")],
    )
    remediate = StageExecution(
        ref_id="remediate", type="remediate", name="Remediate",
        requisite_stage_ref_ids={"approve"}, context=dict(base),
        tasks=[_task("remediate", "remediate")],
    )
    verify = StageExecution(
        ref_id="verify", type="verify", name="Verify",
        requisite_stage_ref_ids={"remediate"}, context=dict(base),
        tasks=[_task("verify", "verify")],
    )
    postmortem = StageExecution(
        ref_id="postmortem", type="postmortem", name="Post-Mortem",
        requisite_stage_ref_ids={"verify"}, context=dict(base),
        tasks=[_task("postmortem", "postmortem")],
    )

    return Workflow.create(
        application="incident-commander",
        name="Autonomous Incident Commander",
        stages=[triage, *diagnostics, root_cause, approval, remediate, verify, postmortem],
    )


def _stream(execution_id: str, stop: threading.Event) -> None:
    stream = WorkflowStream(execution_id)

    def on_event(item) -> None:
        if item.event_type == "custom.progress":
            agent = item.data.get("agent", "?")
            print(f"    {agent:>16} │ {item.data.get('message', '')}")
        elif item.event_type in {"workflow.completed", "workflow.failed"}:
            print(f"    {'—' * 16} ┴ {item.event_type}")

    stream.on_event(on_event)
    stop.wait()
    stream.close()


def _auto_approver(store, queue, execution_id: str, stop: threading.Event) -> None:
    """Stand in for the on-call human: approve each remediation as it comes up."""
    last_approved: set[str] = set()
    while not stop.wait(0.3):
        try:
            wf = store.retrieve(execution_id)
        except Exception:
            continue
        gate = next((s for s in wf.stages if s.ref_id == "approve"), None)
        if gate and gate.status == WorkflowStatus.SUSPENDED and gate.id not in last_approved:
            print(f"    {'on-call':>16} │ ✋ approving proposed remediation")
            approve(queue, execution_id, gate.id, {"user": "on-call-engineer"})
            last_approved.add(gate.id)
        elif gate and gate.status != WorkflowStatus.SUSPENDED:
            last_approved.discard(gate.id)   # re-arm for the next loop's suspension
        if wf.status.is_complete:
            return


def audit(workflow, store, event_store) -> bool:
    print("\n=== Incident review ===")
    ok = workflow.status == WorkflowStatus.SUCCEEDED
    print(f"  workflow status : {workflow.status.name}")
    print(f"  remediations    : {len(INCIDENT['actions'])} -> {INCIDENT['actions']}")
    print(f"  final health    : {'HEALTHY' if INCIDENT['recovery_level'] >= 2 else 'DEGRADED'}")

    stages = {s.ref_id: s for s in workflow.stages}
    diag_done = sum(1 for i in range(NUM_DIAG) if stages.get(f"diag_{i}") and stages[f"diag_{i}"].status.is_complete)
    print(f"  diagnostics     : {diag_done}/{NUM_DIAG} agents reported (quorum was 2)")

    try:
        rebuilt = EventReplayer(event_store).rebuild_workflow_state(workflow.id)
        match = str(rebuilt.get("status")) in {workflow.status.name, str(workflow.status)}
        print(f"  event replay    : {rebuilt.get('status')} ({'matches live state' if match else 'MISMATCH'})")
        ok = ok and match
    except Exception as e:
        print(f"  event replay    : skipped ({e})")

    report = OUT_DIR / "postmortem.md"
    if report.exists():
        print(f"  post-mortem     : {report} ({len(report.read_text())} bytes)")
    else:
        print("  post-mortem     : MISSING")
        ok = False

    print(f"\n  RESULT: {'RESOLVED ✅' if ok else 'FAILED ❌'}")
    return ok


def main() -> int:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    reset_environment()

    have_key = "yes" if os.getenv("OLLAMA_API_KEY") else "no (offline stubs)"
    print("=== Autonomous Incident Commander ===")
    print(f"  alert : {ALERT}")
    print(f"  model : {os.getenv('INCIDENT_MODEL', 'glm-5.2')}  |  OLLAMA_API_KEY: {have_key}\n")

    reset_event_bus()
    url = f"sqlite:///{OUT_DIR}/incident.db"
    store = SqliteWorkflowStore(url, create_tables=True)
    queue = SqliteQueue(url)
    queue._create_table()
    event_store = SqliteEventStore(url, create_tables=True)
    configure_event_sourcing(event_store)
    registry = build_registry()

    workflow = build_workflow()
    store.store(workflow)

    stop = threading.Event()
    threading.Thread(target=_stream, args=(workflow.id, stop), daemon=True).start()
    threading.Thread(target=_auto_approver, args=(store, queue, workflow.id, stop), daemon=True).start()

    Orchestrator(queue, store=store).start(workflow)
    processor = QueueProcessor(queue, store=store, task_registry=registry)
    processor.start()

    deadline = time.time() + 420
    while time.time() < deadline:
        if store.retrieve(workflow.id).status.is_complete:
            break
        time.sleep(0.4)
    processor.stop()
    stop.set()

    return 0 if audit(store.retrieve(workflow.id), store, event_store) else 1


if __name__ == "__main__":
    raise SystemExit(main())
