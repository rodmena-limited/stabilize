# #45 — `stabilize[c]`: the compiled psycopg driver against the system libpq

Ticket: issuedb #45. Released in 0.30.0.

## EARS spec

- Where the `c` extra is selected, stabilize shall declare `psycopg[c]`, so the compiled driver links against the system libpq.
- The stabilize package shall not declare `psycopg[binary]` in any extra.

## Synthesis

Packaging extras. The estate precedent is `auth[c]` and `runflow[c]`, so the name `c` is kept identical rather than invented. resilient-circuit uses `postgres-c` for the same purpose; that is theirs.

## Alternatives

- **An opt-in `c` extra [CHOSEN]** vs making `psycopg[c]` unconditional [REJECTED: it needs libpq headers and a compiler at install time] vs `psycopg[binary]` [REJECTED: bundles its own libpq and takes the TLS verifier out of the operator's hands] vs documentation only [REJECTED: does not execute].
- `c` is not added to `all`, so `all` stays installable without build tooling.

## Verification

In clean venvs from the built wheel:
- `stabilize[c]` → `psycopg.pq.__impl__ == "c"`, with the system libpq version.
- Plain `stabilize` → `python`.
- No `psycopg-binary` in either.
