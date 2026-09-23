# #51 — PostgreSQL pool holders leak; close() never releases the pool

Ticket: issuedb #51. Released in 0.29.1.

## EARS spec

- The PostgreSQL queue shall hold exactly one reference to its connection pool for its lifetime.
- The PostgreSQL workflow store and event store shall each hold exactly one reference to their connection pool for their lifetime.
- When `close()` is called on a queue, workflow store or event store, the connection manager shall release exactly that owner's hold on exactly that pool.
- If `close()` is called more than once on the same owner, then the connection manager shall release nothing further.
- If an owner releases a pool that other owners still hold, then the connection manager shall keep that pool open.
- If an owner releases a pool that no other owner holds, then the connection manager shall close that pool.
- If a queue is closed, then the connection manager shall not release any hold on a pool with the same DSN but different pool options.
- If the connection manager is asked to release a pool it does not manage, then it shall change nothing.

## Technical problems

1. Reference-counted resource lifetime with several owners of one shared resource.
2. Releasing by identity rather than by lookup key, so that one owner's release cannot affect another owner's pool.
3. Making release idempotent per owner.

## Solution domains

- Reference counting (acquire once, release once), as in the RAII / ownership literature. Each owner's `acquire` is paired with its `release`.
- The codebase's existing `ConnectionManager` holder table (`_postgres_holders`, keyed by `(dsn, options.key())`).

## Alternatives

- **Acquire the pool once at construction and release by pool identity [CHOSEN].** Every owner gets one hold, and the release is precise to the key, so nothing else is touched.
- Stop holder counting and return to one pool per DSN, never closed until process exit [REJECTED]. This would reintroduce the bug 3bfa497 fixed: a store and a queue on one DSN closing each other's pool.
- Keep a hold per operation and release in a `finally` after each operation [REJECTED]. It adds 14 acquire/release pairs to hot paths, and a single missed pair reintroduces the leak.
- Keep `close_postgres_pool(dsn)` as the release path [REJECTED]. It decrements every key for the DSN, which closes pools that other owners hold with different options.
- Make repeated `close()` safe by checking the holder count [REJECTED]. The count cannot tell this owner's hold apart from another owner's. The per-owner guard (`release_pool_once`) can.

## Verification

- `tests/test_queue_pool_holder.py`, 9 tests with no database. Against 0.29.0: 7 failed and 2 passed, and the 2 that passed are controls that show the holder table can be read and does increase. Against the fix: 9 passed.
- Real PostgreSQL (testcontainers): on 0.29.0, holders went 5 → 6 over 15 operations, and after one `close()` there were 5 holders with the pool still open. After the fix: 1, 1, 1, then after `close()` the table was empty and the pool closed.
- Affected range: holder counting was introduced in 3bfa497 (v0.25.0). `close()` at `3bfa497^` closed by DSN with no count. So 0.25.0 – 0.29.0 are affected, and 0.24.x and earlier are not.
