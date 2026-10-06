# 68 — RetryableTask per-call and lifecycle limits are separate

Ticket: #68 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- Where a RetryableTask declares timeouts, the per-call limit (`get_execution_timeout(stage)`) and the total lifecycle limit (`get_timeout()` / `get_dynamic_timeout(stage)`) shall be separately settable; the per-call limit defaults to the lifecycle limit.
- The documentation shall state that exceeding either calls `on_timeout(stage)` and that `None` from it completes the task TERMINAL.

## Synthesis

- Alternatives: new overridable method with backwards-compatible default [CHOSEN] vs changing get_timeout's meaning [REJECTED: silent behaviour change for every existing task].

## Evidence

`tests/test_retryable_call_timeout.py`: a 1 s call with a 200 ms per-call limit reaches on_timeout on the fix and ran to completion on 0.31.0; default-unchanged control green on both.
