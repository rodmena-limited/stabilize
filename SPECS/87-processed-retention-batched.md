# 87 — processed_messages retention in bounded batches on its own thread; one-shot CLI (C3, #143)

Ticket: #87. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: round 1b (pipeline 2933): no poll stall reproduced on PostgreSQL with 2 M rows (0.54 s), SQLite 1.95 s against a 2.0 s bound; the PostgreSQL backlog was NOT drained in 120 s (sampled stale ids still processed) — cause under diagnosis in round 1c. CONFIRMED by reading and by ci.rodmena.co.uk's production numbers (#143: 3,903,663 rows, 3,283,697 older than 168 h, statement_timeout 0 -> one unbounded DELETE). Probe probe_retention_does_not_stall.py (poll stall and backlog drain) pending CI after a fixture fix (round 1b, pipeline 2933). Peer #143 / v3 #7a.

## EARS spec

- The processed-message retention sweep shall delete in batches of retention_batch_size rows (default 10,000), each batch in its own transaction, and stop after retention_max_rows_per_sweep rows (default 200,000) or when no stale row remains; it shall return the total removed.
- The sweep shall run on its own thread, so message polling continues while it runs; a workflow started while a sweep of a 2 M-row backlog runs shall complete within 2.0 s.
- store.cleanup_old_processed_messages(max_age_hours, batch_size=None, max_rows=None) shall delete every stale row in batches when called directly, returning the total (same result as 0.32.1, bounded transactions).
- The CLI shall offer `stabilize prune-processed --db-url --older-than-hours [--batch-size]` for a one-shot backlog drain, printing the total.
- Rows younger than the cutoff shall never be removed.

## Technical problems

1. Bulk delete of millions of rows without one long transaction (lock duration, WAL volume, replication lag, statement_timeout).
2. Maintenance work sharing a thread with latency-sensitive polling.

## Solution domains

- PostgreSQL batched delete by ctid from an indexed range (idx_processed_messages_at); SQLite rowid batches.
- Background maintenance loop (the processor already runs recovery on its own thread).

## Alternatives

- Batched ctid/rowid delete [CHOSEN] vs one DELETE (current) [REJECTED: unbounded transaction] vs table partitioning by processed_at [REJECTED: estate-wide schema migration of a 3.9 M-row table] vs TRUNCATE-and-copy [REJECTED: loses the live dedup window].
- Thread: dedicated retention thread [CHOSEN] vs keep on poll thread with small caps [REJECTED: still stalls polling for the batch time on every sweep].

## Callers (contract change)

- cleanup_old_processed_messages: QueueProcessor poll loop (moves to the retention thread), WorkflowStore interface default (returns 0), PostgreSQL maintenance mixin, SQLite store; external callers keep the same result.

## Die-here / two-at-once

- Die between batches: committed batches stay deleted; the next sweep continues. Two processors sweeping: batches touch disjoint rows or zero rows (deleting an already-deleted ctid is a no-op).
