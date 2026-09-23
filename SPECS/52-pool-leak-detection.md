# #52 — Leak detection: server-side per-tenant probe and a suite-wide pool check

Ticket: issuedb #52. Regression guard for #51.

## EARS spec

- When N distinct tenant DSNs each construct a PostgresQueue and a PostgresWorkflowStore, perform operations, and close them, the PostgreSQL server shall report zero backends for those tenants within 10 s.
- The probe shall fail against stabilize 0.29.0 and pass against the current source.
- When a test finishes and its fixtures have been torn down, the test suite shall fail that test if any PostgreSQL pool is still held in the ConnectionManager.
- The suite-wide check shall run before `SingletonMeta.reset` force-closes the remaining pools.
- The suite-wide check shall be shown to fail against the 0.29.0 queue implementation.

## Technical problems

1. Verifying a resource's lifetime by observing the counterparty (the server's backend count), not the client's own bookkeeping.
2. The test harness force-closed every pool after every test (`reset_connection_manager` → `close_all()`), so no leaked pool could outlive a test and no test could observe a leak.

## Solution domains

- Leak detection by asserting a post-condition at teardown (the pattern used by leak checkers such as pytest's unraisable-exception and thread-exception plugins).
- Observing the counterparty: `pg_stat_activity`, filtered by a per-tenant `application_name`.

## Alternatives

- **Server-side count via `pg_stat_activity` [CHOSEN]** vs the client-side holder table [REJECTED: the holder table is the code's model of itself, and that model was the thing that was wrong in #51].
- **Teardown assertion in the autouse fixture, before the reset [CHOSEN]** vs turning psycopg_pool's deletion warning into an error [REJECTED: psycopg_pool 3.3.0's `__del__` emits no Python warning, only a logged thread-stop timeout, so `filterwarnings` cannot see it].
- **Fail the leaking test [CHOSEN]** vs log it [REJECTED: a leak check that cannot fail the build is the check that missed #51].

## Verification

- `probe_pool_release_per_tenant.py`, 4 tenants, 12 pushes each:
  - v0.29.0: 24 backends while open, 24 after close + 10 s, exit 1.
  - Current source: 24 while open, 0 after close + 10 s, exit 0.
- Current `tests/test_queue.py` run against the v0.29.0 source with the new conftest check: 6 of 12 PostgreSQL cases error, each with the queue's pool still held (holder counts 5–11).
- Full suite on the current source with the per-test check: 1674 passed, 5 skipped, 2 xfailed, and no errors. The second xfail was a leak the check had found: `test_race_between_check_and_mark_demonstrates_bug[postgres]` took two pool holds through `get_postgres_pool()` and never released them, and its `xfail(strict=True)` marker absorbed the teardown failure.
- Because xfail also absorbs teardown failures, leaks are also collected and `pytest_sessionfinish` sets a failing exit status. Known-positive (the race test as it was at HEAD): pytest printed "2 xfailed", the session named the leaking test (holder counts [2]), exit 1. After the fix (the test uses `repository._pool`): 13 passed, 2 skipped, 1 xfailed, exit 0.
- The full suite was NOT re-run after the session hook and the race-test fix. Those were checked with the targeted runs above.
