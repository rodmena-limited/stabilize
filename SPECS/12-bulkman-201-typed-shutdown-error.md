# SPEC 12 — Adopt bulkman 2.0.1: classify shutdown races via typed `BulkheadShutdownError`

- **Ticket:** issuedb #12
- **Status:** closed (verified against the published 2.0.1 wheel)
- **Origin:** agent-mail `fix-notice` from bulkman, id `01KZ2P2CFH4P399V79P7DDQCCP`,
  thread `thr-de310158274547d583e4`, responding to the observation Stabilize raised
  when adopting 2.0.0 (issuedb #10 / SPEC 10).

## Context

Adopting bulkman 2.0.0 (SPEC 10) surfaced an untyped signal: a post-shutdown execute
raised a bare `RuntimeError` leaked from `concurrent.futures`, which a caller cannot
distinguish from a `RuntimeError` raised by the task itself. That distinction is
load-bearing for Stabilize — "the manager is shutting down" must be **retryable**
(leave the message for redelivery) while a task's own failure must be **permanent**
(fail it, consume the attempt).

We worked around it in SPEC 10 by tracking terminal state ourselves
(`TaskBulkheadManager.is_shutdown`) and gating a bare `except RuntimeError` on that
flag. bulkman reports the same defect was raised independently by another consumer
and has added `BulkheadShutdownError(BulkheadError)`, raised by all three
implementations on post-shutdown execute, shipping as 2.0.1.

### What is dropped and what is kept

bulkman suggested the terminal-state tracking can now be removed entirely. Only half
of it should be:

- **Dropped:** the bare `except RuntimeError` race net in `execute_with_resilience`.
  It existed solely because the signal was untyped, and is dead code once a typed
  exception exists.
- **Kept:** the pre-dispatch `is_shutdown` check. It does something the exception
  cannot: it refuses *before* the call enters the circuit breaker. A refusal raised
  from inside the protected call is recorded by `WorkflowCircuitFactory` as a failure
  against that workflow's circuit, so a rolling deploy could trip circuits for
  workflows that never failed and leave them tripped through the cooldown. Catching
  the typed exception is the correct fallback for the race; not entering the circuit
  at all is the correct primary path.

## EARS requirements

- The Stabilize package shall declare a bulkman requirement whose floor is at least
  `2.0.1`, so the typed shutdown exception is guaranteed present rather than merely
  likely.
- When a bulkhead raises `BulkheadShutdownError`, the `execute_with_resilience`
  function shall raise a `TransientError` so the message is redelivered rather than
  the task's attempts being consumed.
- The `execute_with_resilience` function shall classify a shutdown race on the
  exception type alone and shall not inspect exception message text.
- The `execute_with_resilience` function shall no longer catch bare `RuntimeError` to
  detect a shutdown race, that workaround being obsolete once bulkman raises a typed
  exception.
- While a bulkhead manager is already terminal, the `execute_with_resilience` function
  shall refuse dispatch before the call enters the circuit breaker, so that a shutdown
  is not recorded as a failure against a workflow circuit.
- If a task raises its own exception on a live manager, then `execute_with_resilience`
  shall not classify it as a shutdown race.
- The resilience test suite and the full unit suite shall pass against the bulkman
  2.0.1 wheel served by PyPI.

## Open question to bulkman — ANSWERED from the published wheel

Asked: does `BulkheadThreading`'s terminal check raise `BulkheadShutdownError`
synchronously, or surface it wrapped in an `ExecutionResult` as a task failure would?

**Answer: both paths raise synchronously.** Probed against 2.0.1 before any code
change — the deterministic path (shutdown, then execute) and the submit-race path
each raise `BulkheadShutdownError` on the calling thread. A live task failure still
returns `ExecutionResult(success=False, error=BulkheadError(...))`, and
`BulkheadError` is the parent of `BulkheadShutdownError`, not its subclass, so it can
never be caught by the shutdown clause.

`BulkheadShutdownError` is **not** a `RuntimeError` subclass. That is pinned by
`test_shutdown_error_is_typed_not_a_bare_runtime_error`: if it ever became one, a
task's own `RuntimeError` and a shutdown race would be indistinguishable again and
the retryable/permanent split would break silently.

## Measurement behind the "kept" decision

The circuit-tripping claim above was verified rather than argued, with a control
proving the check can go both ways: a `BulkheadShutdownError` propagating through the
`@circuit`-protected call **opens the circuit after 2 occurrences**; an ordinary
failure opens it too (control). Pinned by
`test_shutdown_does_not_trip_the_workflow_circuit`, which fails with
`ProtectedCallError` when the pre-dispatch guard is removed.

## Verification

Against the wheel PyPI actually serves — not the changelog, not a local checkout.
Each "shall" line is checked, and every new or changed check is falsified (reverted to
confirm it goes red) before the ticket closes.
