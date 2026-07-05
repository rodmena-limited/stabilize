# Autonomous Incident Commander

An AI on-call engineer that works a production incident from alert to
post-mortem, built on stabilize. It is a self-contained showcase: the
"production environment" is simulated with deterministic tools (so the run is
reproducible), while the reasoning — triage, diagnosis, root-cause consensus,
and the write-up — is done by a real model (ollama.com cloud `glm-5.2`).

## The incident

The `checkout` service is failing: a bad deploy (`v2.4.1`) introduced payment
timeouts. Recovery takes two steps — roll back the deploy, then restart the
payment connection pool — so the service only returns healthy after both. That
second step is what drives a real self-healing loop: the first remediation is
not enough, verification catches it, and the commander re-investigates.

## The workflow

```
triage (tool-using agent: confirm + set severity)
  ├── diagnose: error logs   ┐
  ├── diagnose: metrics      ├─ 3 parallel ReAct agents, each with read-only tools
  └── diagnose: deploys      ┘
        │  (proceed on a quorum of 2, gather findings with a reducer)
        ▼
   root cause (consensus: root cause + proposed fix + risk)
        ▼
   human approval of the remediation   (durable HITL gate)
        ▼
   remediate (privileged action — actually changes the environment)
        ▼
   verify ── still degraded? ──▶ loop back to triage (bounded self-heal)
        │  healthy
        ▼
   post-mortem (the model writes the report)
```

Each behaviour is a plain property or return value:

- **Parallel tool-using agents** — `DiagnosticAgent(AgentLoopTask)` with a
  `ToolRegistry` of read-only environment tools.
- **Consensus on a quorum** — the root-cause stage uses
  `JoinType.N_OF_M, join_threshold=2` and `output_reducers={"finding": "collect"}`
  to gather each agent's finding into a list.
- **Human-in-the-loop** — an `ApprovalTask` gate suspends until the on-call
  engineer approves (here, an auto-approver stands in).
- **Self-healing loop** — `verify` returns `TaskResult.jump_to("triage", ...)`
  when the service is still degraded, bounded by a remediation cap.
- **Live streaming** — every task calls `emit_progress`, printed like an
  incident channel via `WorkflowStream`.
- **Durable + audited** — every step commits atomically and is event-sourced;
  the run ends by replaying the event log and confirming it reconstructs the
  same final state.

## Run it

```bash
export OLLAMA_API_KEY=...     # optional; deterministic offline stubs otherwise
python examples/incident_commander/main.py
```

The post-mortem is written to `/tmp/stabilize-incident/postmortem.md`
(override with `INCIDENT_DIR`).

## Files

- `tools.py` — the simulated, stateful production environment and its tools.
- `incident_tasks.py` — the triage agent, diagnostic agents, root-cause
  consensus, remediation, self-healing verify, and post-mortem tasks.
- `main.py` — the DAG, streaming, the stand-in on-call approver, and the review.
