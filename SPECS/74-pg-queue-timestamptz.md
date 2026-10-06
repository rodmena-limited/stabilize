# 74 — PostgreSQL queue timing is absolute

Ticket: #74 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- While two sessions have different TimeZone settings, the PostgreSQL queue shall deliver, delay and lock messages identically for both.
- While the server's TimeZone observes DST, a DST change shall not shorten or lengthen any delay or lock.
- The PostgreSQL queue shall take lock, renewal and reschedule times from the database clock only.

## Synthesis

- Alternatives: TIMESTAMPTZ columns [CHOSEN] vs forcing SET TimeZone='UTC' per connection [REJECTED: overrides the caller's session, existing rows stay wrong] vs documenting 'run UTC' [REJECTED: DST on a non-UTC server still skews].
- processed_messages.processed_at stays TIMESTAMP: retention compares it with the same session clock, and a rewrite of a large table would block handlers.
- DST variant: SUSPECTED by mechanism, not reproduced.

## Evidence

`probe_pg_queue_session_timezone.py` (New York vs UTC sessions): 0.31.0 4 h stall, 2 h delay delivered at once, a locked message re-polled; fixed all pass.

## Second falsification pass

- The migration reads existing TIMESTAMP values in the migrating session's TimeZone; correct only if workers wrote them in that zone. Documented in the changelog (drain the queue or migrate with the workers' zone).
