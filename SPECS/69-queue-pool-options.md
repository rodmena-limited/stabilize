# 69 — PostgresQueue accepts PoolOptions

Ticket: #69 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- `PostgresQueue` shall accept the same `PoolOptions` as `PostgresWorkflowStore`; the same instance on both shall yield one pool.
- The documentation shall state the idle transaction cost of the poll interval.

## Synthesis

- Alternatives: `options=` merged with `schema=` via `with_schema` [CHOSEN: identical to the store] vs a shared pool object parameter [REJECTED: bypasses the pool registry and its release accounting].

## Evidence

`tests/test_queue_pool_options.py`. Idle cost measured on PostgreSQL 16 (pg_stat_database, own database): 19.6 xact/s at 50 ms, 1.2 at 1000 ms; documented in docs/guide/persistence.rst.
