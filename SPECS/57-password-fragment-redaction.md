# #57 — A password fragment libpq echoes back is redacted

Ticket: issuedb #57. Released in 0.30.2.

## EARS spec

- If a connection string cannot be parsed, then the error stabilize raises shall not contain any fragment of the password, including a fragment libpq quotes back after a space.
- A libpq diagnostic that contains no password fragment shall be kept.

## Measured on 0.30.1

- `host=h password=Zq7Sentinel Pw9xK dbname=d` produced the libpq error `missing "=" after "Pw9xK" in connection info string`. stabilize's ValueError carried `"Pw9xK"` unchanged: `redact_text` rewrote only URL userinfo and `password=<token>`, and the echoed fragment is neither.
- Passwords containing quotes or backslashes parse and do not leak.

## Alternatives

- **Scrub the password fragments taken from the source string [CHOSEN].** It removes exactly what is secret and keeps diagnostics such as `invalid connection option "HOST"`.
- vs dropping libpq's message entirely [REJECTED: it loses the diagnostic an operator needs; the over-redaction failure found by `probe_dsn_ssl_params` in 0.30.0].
- vs pattern-matching the message alone [REJECTED: the echoed fragment has no marker saying it is a password].
- Short fragments (under 4 characters) are scrubbed only where libpq quotes them, so a two-letter piece of a password does not erase every occurrence of those letters.

## Verification

`tests/test_conninfo_parse_redaction.py`:
- 3 cases of a space-split password, each confirming libpq's raw error contains a quoted fragment before asserting stabilize's error does not. On 0.30.1 source all 3 fail; on the fix they pass.
- A secret-free diagnostic (`HOST`) is kept.
