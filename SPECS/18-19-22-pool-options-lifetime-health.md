# Spec: PostgreSQL pool options, holder-counted lifetime, bounded health check

Tickets #18, #19, #22. Implemented together because all three modify
ConnectionManager.get_postgres_pool / close_postgres_pool.

## EARS requirements

- The ConnectionManager shall accept caller-supplied connection options (libpq
  connect kwargs and/or a psycopg pool configure callback) when creating a
  PostgreSQL pool. (#18)
- Where no options are supplied, the engine shall document that its connections
  inherit the server defaults and therefore carry no statement_timeout and no
  lock_timeout. (#18)
- If more than one component holds a PostgreSQL pool for a given connection
  string, then closing one component shall not close the pool the others are
  using. (#19)
- The engine shall close a pool once, and only once, every holder has released
  it, so pools are still reclaimed. (#19)
- If the database is unreachable, then is_healthy() shall return False within a
  bounded, configurable interval rather than blocking for the pool's
  acquisition timeout. (#22)

## Design

`PoolOptions` (frozen dataclass): min_size, max_size, acquire_timeout,
connect_kwargs, configure. Pools are keyed by (connection_string,
options.key()) so differing options produce different pools rather than
silently sharing whichever was created first.

Holder counting: get_postgres_pool registers a holder; close_postgres_pool
decrements and closes only at zero. close_all() clears everything.

is_healthy(timeout) borrows with an explicit timeout, default
DEFAULT_HEALTH_TIMEOUT_SECONDS = 2.0.

## Verified live (audit/evaluations/probe_pool_options.py)

Real PostgreSQL 16 container, baseline asserted FIRST so the check could fail:

    baseline, no options        statement_timeout=0, lock_timeout=0
    connect_kwargs options      statement_timeout=8s, lock_timeout=4s
    configure callback          application_name set per connection
    shared pool, one close      surviving holder still usable
    shared pool, last close     pool actually closed (both directions)
    is_healthy reachable        True in 0.007s
    is_healthy unreachable      False in 1.000s (was ~30s)

## Gotcha recorded on PoolOptions.configure

psycopg_pool discards any connection a configure callback leaves INTRANS. A
callback issuing SET without a commit destroys every connection and borrows
then fail with PoolTimeout. Found by this probe's own first draft.
