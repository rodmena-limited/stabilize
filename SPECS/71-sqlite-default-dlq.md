# 71 — SQLite create_tables provides the default DLQ table

Ticket: #71 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- When `SqliteWorkflowStore(create_tables=True)` initialises a database, it shall create `queue_messages_dlq`.

## Synthesis

- Alternatives: forward SQLite migration 3 [CHOSEN: covers existing databases] vs editing the baseline SCHEMA [REJECTED: stamped baseline never re-runs] vs DDL in SqliteQueue's constructor [REJECTED: DDL on every construction].

## Evidence

`probe_sqlite_dlq_exists.py`: 0.31.0 docstring setup raised `no such table: queue_messages_dlq` on a poison message; control with `_create_table()` passed; fixed both pass. `tests/test_sqlite_default_dlq.py`.
