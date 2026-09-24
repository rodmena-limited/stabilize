# 55 — CLI accepts keyword/value conninfo and Unix-socket URLs

Ticket: #55 (released in 0.31.0)

## EARS spec

- Where MG_DATABASE_URL or --db-url is a libpq keyword/value conninfo string, mg-up and mg-status shall connect with it.
- Where the URL names a Unix-socket host (`postgresql:///db?host=/var/run/postgresql`), mg-up and mg-status shall connect through the socket.
- If the string cannot be parsed, then the CLI shall print a redacted error and exit 1.

## Reports

- trace-thinkpad-83589d (2026-09-19): `host=... port=5432 dbname=trace sslmode=verify-full user=... sslcert=...` refused with `Invalid database URL`, while migretti accepts it.
- mail-api-f3dc60 (2026-09-23): `postgresql:///appdb?host=/var/run/postgresql` and `postgresql:///appdb` refused.

## Cause

`cli/config.py` `parse_db_url` was a regex requiring `scheme://...host.../dbname`.

## Synthesis

- Technical problem: parsing a PostgreSQL connection string in both libpq forms.
- Solution domain: the libpq conninfo grammar; `psycopg.conninfo.conninfo_to_dict` is the libpq parser already used by the engine (#53).
- Alternatives:
  - libpq parser for strings the URL regex does not match [CHOSEN]: identical acceptance to what the engine and migretti connect with; the classic URL path is unchanged.
  - Extend the regex [REJECTED]: a second grammar that drifts from libpq; every divergence is a URL one tool accepts and the other refuses, which is the reported defect.
  - Replace the regex with libpq for every string [REJECTED for 0.31.0]: changes the parsed result of URLs that work today (defaulted user/port), a behaviour change with no report behind it.
- The URL regex host group no longer admits `/` or `@`, so `postgresql://u@/appdb?host=/tmp` is not read as host `u@`.
- `schema=` is stabilize's own parameter, stripped from the URL query or keyword list before libpq sees it.

## Verification

- `tests/test_cli_conninfo_forms.py`: keyword form keeps every parameter and the schema; socket URL with and without `host`; user without host; classic URL unchanged; unparseable string exits 1 without the password; live `mg_status` against PostgreSQL 16 with a `make_conninfo` keyword string.
- Red on 0.30.4 `cli/config.py`: 5 failed.
- Socket forms verified at parse level only; no Unix-socket server was exercised.
