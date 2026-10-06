# 65 — Orchestrator.start raises when it cannot store the workflow

Ticket: #65 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- If storing the workflow fails for any reason other than it already existing, then `Orchestrator.start` shall raise and shall not push StartWorkflow.

## Synthesis

- Alternatives: store only if absent, re-raise unless it exists afterwards [CHOSEN: backend-agnostic, tolerates a concurrent store] vs catching backend-specific unique-violation types [REJECTED: one exception type per driver, misses the memory/custom stores].
- Callers checked: launcher.py (two sites, pre-store, propagate), tasks/sub_workflow.py (pre-stores, catches -> TERMINAL).

## Evidence

`probe_orchestrator_start_store_failure.py`: 0.31.0 raised nothing and queued StartWorkflow for a missing workflow on both backends; fixed raises, queues nothing. `tests/test_orchestrator_start_store_failure.py`.
