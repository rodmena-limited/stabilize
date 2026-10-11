# 79 — The lock heartbeat keeps renewing after a failed renewal (C27)

Ticket: #79. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (pipeline 2930): one extend_lock exception during a 4 s task with a 1 s lock let a second worker process execute the task again (postgres executions=2; no-fault control 1). SQLite did not reproduce. Probe: audit/evaluations/probe_heartbeat_survives_blip.py.

## EARS spec

- While a handler is executing, the lock heartbeat shall keep renewing the message's queue lock until the handler finishes, whatever number of renewal attempts raise.
- If a renewal attempt raises, then the heartbeat shall log a warning and retry within min(interval, 1 s), and shall stop only when the handler finishes or extend_lock reports the message gone (False).
- The probe shall show 1 execution with one renewal raising, on both backends.

## Technical problems

1. Lease renewal under transient storage faults: a single failed renewal must not end renewal.
2. (Documented limit, not fixed here) A renewal outage longer than the lock duration still lets another worker take the message; at-most-once across that window needs the opt-in task lease.

## Solution domains

- Lease / heartbeat with retry (as in leader-election leases: renewal retries until the deadline). Codebase: queue/processor/lease_guard.py start_lock_heartbeat.

## Alternatives

- Retry on error until stopped [CHOSEN] vs stop on first error (current) [REJECTED: one blip -> duplicate execution, confirmed] vs abort the handler when a renewal fails [REJECTED: the task's side effect is already in progress; aborting does not undo it and turns a blip into a failure].

## Callers

- start_lock_heartbeat: queue/processor/processor.py _start_lock_heartbeat (the only caller). Its return contract (stop event or None) is unchanged.

## Die-here / two-at-once

- Handler finishes while a renewal is in flight: stop event set; the in-flight extend may extend an acked (deleted) row: rowcount 0 -> False -> thread exits.
