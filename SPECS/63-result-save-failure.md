# 63 — A failed save of a successful result neither fails nor re-runs the task

Ticket: #63 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- If saving a successful task's result fails, then the engine shall not record the task as failed and shall not execute the task again in that worker process.

## Synthesis

- Technical problem: an exception classifier that also wraps the persistence step.
- Solution domain: separate task-error classification from result persistence; hold the computed result for the redelivery (in-process outbox).
- Alternatives: move the save outside the classifier and hold the result for redelivery [CHOSEN] vs retry the save in a loop inside the handler [REJECTED: holds the worker and the lock while the database is down] vs persist the result elsewhere first [REJECTED: needs the same database].
- Not covered: a redelivery that lands on a different worker process executes the task again (at-least-once across processes).

## Evidence

`probe_result_persist_failure.py` (fault at `store.transaction`): 0.31.0 TERMINAL on all four faults, PoolTimeout re-ran the task 10x; fixed SUCCEEDED with 1 execution, sqlite + postgres. `tests/test_result_persist_failure.py`.
