# Widen resilient-circuit pin to admit 0.7.x; unblock bulkman 2.0.2 + TLS breaker storage

Ticket: issuedb #14
Status: in-progress
Reported by: conductor-578aaf (RODMENA CI) via AgentBus, 2026-08-10

## Context

stabilize 0.21.0 pins `resilient-circuit>=0.4.6,<0.5`. The published pins of
the house libraries are:

- bulkman 2.0.1 → `resilient-circuit[postgres]>=0.4.6,<0.5`
- bulkman 2.0.2 (latest) → `resilient-circuit[postgres]>=0.5.0,<0.6`
- resilient-circuit latest → 0.7.0

Consequences, verified against PyPI and the resilient-circuit source:

1. stabilize 0.21.0 and bulkman 2.0.2 are mutually unsatisfiable (stabilize
   caps RC below 0.5; bulkman 2.0.2 floors it at 0.5.0). A project depending on
   both at their latest versions cannot resolve; in practice stabilize silently
   forces bulkman back to 2.0.1.
2. The `<0.5` cap makes RC 0.7.0 unreachable. RC_DB_DSN (introduced in 0.7.0)
   is the only way to express TLS parameters (sslmode, sslcert, sslkey,
   sslrootcert) to the breaker's PostgreSQL storage. Without it, a breaker on a
   TLS-mandatory database cannot connect and resilient-circuit silently falls
   back to in-memory storage — breaker state evaporates on every restart.

The `<0.5` cap is stale, not deliberate: the comment in pyproject.toml says the
floor was copied from bulkman 2.0.0's constraint.

A second defect discovered during verification: stabilize's own
`_create_storage()` in `src/stabilize/resilience/circuits.py` hand-parses the
postgres URL into libpq conninfo and drops the query string, so TLS parameters
in `MG_DATABASE_URL` are destroyed even when RC 0.7.0 is installed. RC's
`PostgresStorage` passes its connection string verbatim to `psycopg.connect()`
(both 0.4.7 and 0.7.0), and psycopg3 accepts `postgresql://` URLs with query
params natively, so the hand-parsing is unnecessary and harmful.

## EARS Requirements

- R1 (Event-driven): When the stabilize package is installed with a resolver
  that also considers the latest bulkman release (2.0.2, which requires
  resilient-circuit >=0.5.0,<0.6), then the stabilize dependency
  specification shall declare a resilient-circuit range that is satisfiable
  together with bulkman 2.0.2's range.
- R2 (Event-driven): When the stabilize package is installed and the latest
  resilient-circuit release (0.7.0, which introduces RC_DB_DSN / RC_DB_SSL*)
  is available, then the stabilize dependency specification shall admit
  resilient-circuit 0.7.x.
- R3 (Ubiquitous): The resilient-circuit Python APIs that stabilize imports
  (CircuitProtectorPolicy, ExponentialDelay, RetryWithBackoffPolicy,
  ProtectedCallError, RetryLimitReached, storage.CircuitBreakerStorage,
  InMemoryStorage, PostgresStorage) shall continue to exist with the same
  signatures when stabilize runs against resilient-circuit 0.7.x.
- R4 (Ubiquitous): When a postgresql:// database URL carries TLS query
  parameters (sslmode, sslrootcert, sslcert, sslkey), the stabilize circuit
  breaker storage shall pass those parameters through to the resilient-circuit
  PostgreSQL storage rather than dropping them.
- R5 (Ubiquitous): The stabilize workflow engine's existing behavior (SQLite /
  Postgres / memory backends, per-workflow circuit breakers, deadlock retry
  policies) shall remain correct when the widened pin resolves.

## Verification

- R1: `pip install --dry-run "bulkman==2.0.2" "resilient-circuit>=0.4.6,<0.8"`
  resolves to bulkman 2.0.2 + resilient-circuit 0.5.0 — previously
  unsatisfiable. ✅
- R2: `SpecifierSet('>=0.4.6,<0.8').contains('0.7.0')` is True; standalone
  constraint admits RC 0.7.0. ✅
- R3: signature inspection of RC 0.7.0 — all stabilize import sites
  (CircuitProtectorPolicy, ExponentialDelay, RetryWithBackoffPolicy,
  ProtectedCallError, RetryLimitReached, InMemoryStorage, PostgresStorage)
  present with compatible signatures. Full suite passes against RC 0.7.0:
  1313 passed, 7 skipped, 1 xfailed. ✅
- R4: `test_postgres_url_passes_tls_params_through` and
  `test_postgres_plus_psycopg_scheme_stripped` — TLS query params survive
  `_create_storage` verbatim. ✅
- R5: `make check` equivalent — ruff check (src+tests+golden) clean, `mypy
  src/` clean, full `pytest` suite green. ✅
