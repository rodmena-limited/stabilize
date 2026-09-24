# #56 — Circuit-breaker storage never logs or raises a DSN password

Ticket: issuedb #56. Released in 0.30.1.

## EARS spec

- If the circuit-breaker database URL cannot be parsed by libpq, then stabilize shall not pass it to resilient-circuit.
- The failure log line and the strict-mode exception shall not contain the DSN password, in the message or in the exception chain.
- Where the failure is an ImportError, the strict-mode exception shall keep its cause.
- Where the URL's scheme is written in upper case, stabilize shall lower-case the scheme before parsing it.

## Measured on 0.30.0

Sentinel password in a `postgresql+asyncpg://` DSN; the detector's known-positive was True.
- stabilize's own ERROR line read `PostgreSQL circuit breaker storage unavailable (ProgrammingError: missing "=" after "postgresql+asyncpg://u:<SENTINEL>@...`.
- Strict mode carried the same text in the message and in `__cause__`.
- resilient-circuit logged it too: 0.7.0 as "Failed to ensure table exists", 0.8.2 as "Failed to verify circuit breaker schema". 0.8.4 logs nothing, but its exception still carries the password.

`RunTaskHandler` always builds a `WorkflowCircuitFactory`, so bulkman's `circuit_breaker_enabled` does not gate this path.

## Why 0.30.0 missed it

The consumer-side sweep matched `logger(..., e)`. It did not match an exception formatted into an f-string argument: `_degrade_or_raise(f"{type(exc).__name__}: {exc}", exc)`.

The re-sweep matched `{e}`, `str(e)` and `repr(e)`: 58 sites, with the known instance among the matches. Only three code paths open a PostgreSQL connection: the pool (guarded by #53), the CLI (keyword parameters, redacted), and this one.

## Alternatives

- **Parse with libpq before `PostgresStorage` [CHOSEN].** resilient-circuit never receives the string, which also protects consumers still on resilient-circuit below 0.8.4.
  - vs redacting only our own log line [REJECTED: resilient-circuit 0.7.0–0.8.3 still log it themselves].
- **No chain for non-import failures [CHOSEN]** vs keeping `from exc` [REJECTED: the chained psycopg error carries the password].
- **Lower-case the scheme [CHOSEN].** The classifier already treats `POSTGRESQL://` as PostgreSQL, and libpq rejects it. The form previously reached `PostgresStorage` only to fail (and leak).
  - Upper-case libpq keywords (`HOST=`) cannot be normalised safely, so they fail loudly with the password redacted.

## Verification

`tests/test_circuit_storage_redaction.py`, 5 cases:
- On 0.30.0 code, 3 failed: the password in the log, the password in the strict-mode error, and resilient-circuit receiving the unparseable string.
- On the fix, all 5 pass.

`tests/test_circuit_storage_honesty.py` has two cases for postgres-intended forms libpq cannot parse. Both are reported at ERROR, and neither is treated as "no database".

`probe_circuit_storage_honesty` passes. The full suite ran on the tree just before the scheme-lowering change (1718 passed, 2 failed on exactly those forms, now fixed); the circuit and resilience test files were re-run afterwards.
