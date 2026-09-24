# #44 — `stabilize mg-check-grants`: can the runtime role reach every engine table?

Ticket: issuedb #44. Released in 0.30.0.

## EARS spec

- The stabilize CLI shall report, for a named database role, which engine tables that role cannot SELECT, INSERT, UPDATE or DELETE.
- When every engine table is reachable by the named role, the CLI shall exit 0 and say so.
- If any engine table is unreachable, then the CLI shall exit 1 and name each table and the missing privileges.
- The CLI shall derive the engine table set from the shipped migrations, not from a hand-maintained list.
- If the role does not exist, or the check cannot confirm a table owner's own access, then the CLI shall exit 2 and shall not report the role as complete.
- The CLI shall report a missing USAGE on each table's serial sequence, because INSERT needs it.

## Technical problems

1. The engine's table set changes with each migration; consumers' grants are a snapshot of it.
2. A per-role privilege question answered from a user that can read the catalogue.
3. Telling "no grants" apart from "the check cannot see anything".

## Solution domains

- `has_table_privilege`, `has_schema_privilege`, `has_sequence_privilege` and `pg_get_serial_sequence` (PostgreSQL manual, System Information Functions).
- Table set parsed from the up-sections of the shipped migrations (`CREATE TABLE`, less a later `DROP TABLE`).

## Alternatives

- **CLI subcommand run at deploy time [CHOSEN]** vs documenting `ALTER DEFAULT PRIVILEGES` [REJECTED: a note does not execute; both reporting consumers already knew it and still had no check] vs an engine self-check at startup [REJECTED: the runtime role often cannot read the catalogue, and failing closed turns a grant gap into an outage].
- **Table set derived from migrations [CHOSEN]** vs a constant in source [REJECTED: a second copy of a fact the migrations already state].
- **Owner control through the same function [CHOSEN]** vs trusting an all-true result [REJECTED: a check that cannot say no cannot say yes].

## Verification

`tests/test_cli_check_grants.py`, run against real PostgreSQL:
- The derived set is 8 tables. A table dropped by a later migration is not required; a down-section drop does not remove a table.
- A fully granted role exits 0.
- A role with no grants exits 1, with 8 tables named.
- One revoked DELETE plus one revoked sequence USAGE exit 1, naming exactly those 2.
- A revoked schema USAGE is reported.
- An unknown role exits 2.
- A wrong schema reports all 8 tables as missing.

Not covered: the event-store tables and `task_leases`. The engine creates those itself at runtime, not through the shipped migrations.
