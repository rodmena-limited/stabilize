# 29 — Commit-ordered delivery for durable subscriptions

Status: in-progress · Ticket: issuedb #29 · 2026-09-19

## EARS

- When a durable subscription polls for new events on PostgreSQL, the engine
  shall deliver every committed event exactly once, regardless of the order in
  which producing transactions committed relative to their sequence numbers.
- The engine shall not advance a subscription cursor past an event whose
  transaction has not yet committed.
- Where events predate the commit-ordering column, the engine shall treat them
  as deliverable rather than skipping them.
- The engine shall continue to deliver events on SQLite unchanged.
- If a future change reintroduces sequence-ordered cursors on PostgreSQL, the
  test suite shall fail.

## Problem

`events.sequence` is `BIGSERIAL`, assigned at INSERT and not at COMMIT. A reader
that advances its cursor to the highest visible sequence steps over a lower
sequence belonging to a still-open transaction; when that transaction commits,
its event is below the cursor and is never delivered. Since events join the
workflow state transaction, those transactions are long-lived, so the window is
wide rather than theoretical.

Reproduced on PostgreSQL 16, and the probe now demonstrates both strategies in
one run:

```
delivered (commit cursor):   ['slow-09a63f', 'fast-90d330']
delivered (sequence cursor): ['fast-90d330']
  -> the sequence cursor lost slow-09a63f
```

SQLite is incidentally safe: its write lock serialises commits, so commit order
is sequence order.

## Resolution

A `commit_xid xid8` column defaulted to `pg_current_xact_id()`, polled against a
watermark of `pg_snapshot_xmin(pg_current_snapshot())` — the lowest transaction
id still in progress, and therefore a monotone, gap-free frontier. `xid8` is
64-bit and never wraps, so the comparisons stay total; the 32-bit `xid` system
column would wrap and must not be used.

Both backends expose `get_events_since_committed(cursor, limit) -> (events,
next_cursor)` and `supports_commit_cursor()`; `SubscriptionManager` uses them
through `hasattr`, falling back to the sequence cursor, so a third-party store is
unaffected.

### Decisions worth recording

- **The `xid8` column is applied only by a version-gated migration, not by
  `CREATE TABLE`.** `xid8` does not exist before PostgreSQL 13, so including it
  in the schema constant would break table creation there outright. Below 13 the
  engine logs a warning naming the loss mode and keeps the sequence cursor.
- **The ALTER is two statements.** A single
  `ADD COLUMN ... DEFAULT pg_current_xact_id()` uses a volatile default and
  rewrites the entire table. Split, the ADD is instant and pre-existing rows keep
  NULL, coalesced to `'0'::xid8` so they stay deliverable.
- **`last_commit_cursor` is appended at the end of `event_subscriptions`, not
  inserted mid-table.** `get_subscription` reads `row[3]`/`row[4]` positionally,
  and `ALTER TABLE` always appends — so a mid-table column would have given a
  freshly created database a different column order from an upgraded one, and
  the positional read would have diverged between them.
- **Both backends carry an idempotent column migration.** The event store's
  tables are created by `CREATE TABLE IF NOT EXISTS` in code and are **not**
  covered by the migretti migrations in `migrations/`, so a schema addition
  otherwise never reaches an existing deployment.

## Cost, stated rather than hidden

Delivery is held behind the oldest in-flight write transaction on the database:
one long workflow transaction stalls subscription delivery. This is the price of
a gap-free frontier and should carry a lag gauge.

## Known gap

The event store has no migration path of its own. This fix carries its own
ALTERs; the general problem needs its own ticket.

## Verification

`audit/evaluations/probe_event_commit_watermark.py`, registered in
`run_all.sh`. It starts a throwaway PostgreSQL container, holds one transaction
open across a poll, and asserts the commit cursor loses nothing and duplicates
nothing — while printing what the sequence cursor would have lost in the same
scenario.
