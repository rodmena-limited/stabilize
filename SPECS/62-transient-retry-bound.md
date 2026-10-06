# 62 — Transient task retries stop at max_attempts

Ticket: #62 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- If a task raises a transient error, then the engine shall retry it at most max_attempts (10) consecutive times and then mark it TERMINAL.
- The backoff shall be computed from the true attempt number.
- When a task returns RUNNING, the consecutive transient-failure count shall reset to 0.

## Synthesis

- Technical problem: a retry counter that does not survive the queue round trip.
- Solution domain: message retry metadata (the count travels with the message payload, separate from the broker's delivery count).
- Alternatives: payload field `retry_count` (already declared on StageLevel, unused by RunTask) [CHOSEN: survives serialization; old workers ignore it, new workers default it to 0] vs keeping `Message.attempts` in the payload [REJECTED: `poll_one` overwrites it with the delivery count, the same field the bug came from] vs a per-task counter in the task row [REJECTED: schema change and a write per retry].
- Assumption made in Farshid's place (unattended session): the bound applies to CONSECUTIVE transient failures; a RUNNING result resets it, so a multi-hour poller with occasional blips is not terminated after 10 cumulative blips. Reversible in `result._handle_running`.

## Evidence

`audit/evaluations/probe_transient_retry_bounded.py`: 0.31.0 4164 (sqlite) / 1412 (postgres) executions in 60 s, workflow RUNNING; fixed TERMINAL after exactly 10. `tests/test_transient_retry_bound.py` red on 0.31.0, green on the fix.
