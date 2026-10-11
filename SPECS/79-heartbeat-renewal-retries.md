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

## Phase 4 design refutation (fresh-context refuter, 2026-10-11) — amendments

- B79-1 (duplicate execution): extend_lock, ack and reschedule are not owner-checked; a retrying heartbeat that outlives its lease extends another worker's lock, and its handler's later reschedule clears that lock (third execution) or its ack deletes the row under the other worker. Design: fence with the poll's attempts counter — extend_lock, ack and reschedule add AND attempts = :attempts on both backends; rowcount 0 means the lease was lost (heartbeat stops; ack/reschedule log and do nothing).
- B79-2: extend_lock returns False on an unparsable message_id, which the heartbeat reads as "gone". Parsing goes through the #81 parser and raises on failure; False is reserved for rowcount 0.

## Implementation (commit 8f40cad) — callers

Callers (grep src/): start_lock_heartbeat <- queue/processor/processor.py:391 (_start_lock_heartbeat) <- :413 process_and_ack; unchanged contract (stop event or None). Fenced operations: ack <- processor.py:416 (after handle), drain.py:62 (process_one), postgres.py:168 / sqlite/queue.py:150 (poll(callback)); reschedule <- processor.py:428 (handler error), :445 (submit failure, message just polled so attempts match), drain.py:68; extend_lock <- lease_guard.beat only. Every caller passes the message object returned by poll_one, whose attempts equals the row's attempts while it holds the lease, so the fence only rejects a worker whose lease another poll took. Messages built without attempts (None) are not fenced. extend_lock now raises on an unparsable message_id instead of returning False (B79-2).
