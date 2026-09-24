# #53 — A malformed DSN is refused before any pool exists

Ticket: issuedb #53. Released in 0.30.0.

## EARS spec

- If a connection string cannot be parsed by libpq, then `get_postgres_pool` shall raise a ValueError before any pool is created.
- The error shall not contain the DSN password in its message or its exception chain.
- No log record shall contain the password.
- A parseable connection string shall create a pool exactly as before.

## Measured

Sentinel password in `postgresql+psycopg://`, detector known-positive True:
- Before: the caller got a PoolTimeout (clean), but 14 of 28 log records from `psycopg.pool` contained the password, one per retry.
- After: a ValueError with the password replaced by `***`, 0 log records, no pool and no hold created.

## Alternatives

- **Parse in `get_postgres_pool` with the libpq parser (`conninfo_to_dict`) and raise `from None` [CHOSEN]**: one chokepoint covers the queue, the workflow store and the event store.
- vs a logging filter on `psycopg.pool` [REJECTED: logging configuration belongs to the consuming application, and a filter misses non-propagating handlers].
- vs rejecting only `+driver` schemes [REJECTED: any unparseable string triggers the same echo].

## Verification

`tests/test_conninfo_parse_redaction.py`: 11 cases, covering both malformed shapes, the keyword form, and the libpq parser as the guard's own known-positive.
