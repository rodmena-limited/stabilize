# 100 — PoolOptions.check and PoolOptions.max_idle reach psycopg_pool

Ticket: issuedb #100. Requested by casebound-audit and casebound-api (2026-10-30).

## EARS spec
- R1. Where PoolOptions.check is set, the engine shall pass it to psycopg_pool.ConnectionPool(check=...), so it runs each time a connection is handed out.
- R2. Where PoolOptions.max_idle is set (seconds, > 0), the engine shall pass it to ConnectionPool(max_idle=...); unset keeps psycopg_pool's default (600 s).
- R3. check and max_idle shall be part of the pool key, so pools with different values are never shared.
- R4. If max_idle is not a positive number, then constructing PoolOptions shall raise ValueError.
- R5. The psycopg-pool floor shall be the first release that accepts check (3.2.0).

## Solution domain
psycopg_pool ConnectionPool parameters (check since 3.2, max_idle). Codebase pattern: PoolOptions fields passed in ConnectionManager.get_postgres_pool.

## Alternatives
- Two explicit fields [CHOSEN]: typed, keyed, documented.
- A free-form pool_kwargs passthrough [REJECTED: would let callers set open/min/max behind the key and break pool sharing guarantees].
