# SPEC 10 — Adopt bulkman 2.0.0 and honor the bulkhead shutdown timeout budget

- **Ticket:** issuedb #10
- **Status:** closed (verified)
- **Origin:** agent-mail `fix-notice` from bulkman, id `01KZ2MDG9MQACR0PCKPQAMD25C`,
  thread `thr-de310158274547d583e4` (bulkman 2.0.0 published to PyPI 2026-08-03).

## Context

`stabilize.resilience.bulkheads.TaskBulkheadManager` wraps
`bulkman.threading.BulkheadThreading`. The declared requirement was
`bulkman>=0.1.0`, so a fresh install or lock refresh would silently resolve to the
2.0.0 major without a deliberate verification pass.

Two 2.0.0 changes touch this repository:

1. `BulkheadThreading.shutdown(wait=True, timeout=...)` now **honors** `timeout`
   instead of blocking until every task finishes. Our shutdown-budget arithmetic
   in `TaskBulkheadManager.shutdown` was therefore decorative under bulkman 1.x
   and becomes load-bearing under 2.0.0 — while `LifecycleManager` never passed a
   budget at all (`lifecycle.py` called `manager.shutdown(wait=True)`), so a
   graceful shutdown advertising a `shutdown_timeout` could block indefinitely.
2. `bulkman` 2.0.0 requires `resilient-circuit>=0.4.6,<0.5`. Stabilize depends on
   `resilient_circuit` directly (`ExponentialDelay`, `WorkflowCircuitFactory`) with
   a floor of `0.4`, which is below what bulkman now demands.

Changes 1, 2, 3, 5 and 6 in the bulkman notice are no-ops for us: we already pass
`circuit_breaker_enabled=False` explicitly, use `BulkheadThreading` (no event
loop), configure `timeout_seconds=14400.0` (never `0.0`), pass valid config
bounds, and use our own `WorkflowCircuitFactory` rather than bulkman's breaker.

## EARS requirements

- The Stabilize package shall declare a bulkman requirement of `>=2.0.0,<3` so that
  adoption of the 2.0.0 major is explicit rather than incidental to a lock refresh.
- The Stabilize package shall declare a resilient-circuit requirement whose floor is
  at least `0.4.6`, matching the floor bulkman 2.0.0 requires, so a resolver cannot
  select a resilient-circuit that bulkman rejects.
- When `LifecycleManager` executes its graceful shutdown sequence, the
  `LifecycleManager` shall pass its remaining shutdown-timeout budget to each
  registered `TaskBulkheadManager.shutdown` call.
- If the remaining shutdown-timeout budget is exhausted when a bulkhead manager is
  reached, then the `LifecycleManager` shall shut that manager down without waiting.
- While running against bulkman 2.0.0, the `TaskBulkheadManager` shall continue to
  construct every bulkhead with `circuit_breaker_enabled=False` and a
  `timeout_seconds` strictly greater than zero.
- The resilience test suite (`tests/test_resilience.py`, `tests/test_bug_fixes.py`)
  and the full unit suite shall pass against the installed bulkman 2.0.0 and its
  required resilient-circuit.
- If a graceful shutdown has already completed, then `TaskBulkheadManager.shutdown`
  shall not raise, notwithstanding bulkman 2.0.0 making `Bulkhead.shutdown` terminal.

## Verification

Each "shall" line is checked against the installed 2.0.0 wheel — not against the
changelog — and the results are recorded in the closing comment on issuedb #10 and
reported back to bulkman on thread `thr-de310158274547d583e4`.
