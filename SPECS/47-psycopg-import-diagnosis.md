# #47 — mg-up and mg-status say which driver they use, and never misdiagnose an import error

Ticket: issuedb #47. Released in 0.30.0.

## EARS spec

- If importing psycopg fails because psycopg itself is absent, then the CLI shall print the install instruction.
- If importing psycopg fails for any other reason, then the CLI shall let the original ImportError propagate unchanged and shall not print the install instruction.
- Before opening a connection, the CLI shall print the psycopg version, its implementation, the libpq version, and whether that libpq is the system one or bundled.
- If the implementation or libpq version cannot be determined, then the CLI shall refuse to connect.
- The CLI shall not print a database error without redacting any DSN password it contains.

## Synthesis

The ImportError subclass is the discriminator: `ModuleNotFoundError` with `name` in {psycopg, psycopg_pool} means genuinely absent; anything else, including a `psycopg_c` version skew, is raised as-is. A bare raise beats a wrong explanation, as migretti shows. Disclosure uses `psycopg.pq.__impl__` and `psycopg.pq.version()`.

## Verification

- `tests/test_cli_psycopg_import.py`: 12 cases.
- Against HEAD, the Deque ImportError produced "psycopg not installed / pip install", exit 1, and no driver line. After the fix it propagates the original message, and the driver line precedes the connection attempt.
- The release gate's `probe_dsn_ssl_params` caught a defect in the first version of `redact_text`. It applied the single-DSN host-less-userinfo rule to the whole message, so any `text: more` became `text:***`: `mg-status` printed `Database error: connection failed:***` and lost the "server does not support SSL" line. No secret leaked, but the diagnostic was destroyed. Fixed by rewriting only the URL tokens inside the text (any run containing `://`) plus keyword secrets. Regression tests: a DSN-free message is returned unchanged, and a malformed `postgres!!!://` scheme is still redacted.
