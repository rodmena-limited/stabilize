# 99 — stabilize's tables can share a host's schema while stabilize's migretti history lives apart; the CLI resolves a URL as libpq does

Ticket: issuedb #99. Reported by trace-thinkpad-83589d and provenance-50ca06 after the 0.33.2 release (2026-10-30).

## Problem
migretti keeps one history table, `_migrations`, per schema, under a fixed name. stabilize 0.33.1's own runner used a separate table (`stabilize_migrations`), so stabilize's tables could live in a host's schema (usually `public`) beside the host's own migretti history. 0.33.2 records stabilize's history in the target schema's `_migrations`, so in that shape it refuses (another project's history) and cannot be pointed elsewhere without moving the tables. Provenance cannot move them: its applied migrations name them as `public.<table>`, add columns, a foreign key and RLS to them, and its fresh install replays from empty.

## EARS spec
- R1. Where a history schema is given (`--history-schema NAME`, `history_schema=` in the URL, or `MG_HISTORY_SCHEMA`), mg-up shall create stabilize's tables in the target schema and keep stabilize's migretti history (`_migrations`, `_migrations_log`) only in the history schema.
- R2. While a history schema is in use, mg-up shall neither read nor write the target schema's `_migrations`, and the host's own `mg verify`, `mg status` and `mg apply` shall behave as if stabilize were absent.
- R3. When no history schema is given and the default history schema (`stabilize_history`) already holds stabilize's history for this target, mg-up and mg-status shall use it.
- R4. When no history schema is given and the target schema holds another project's migretti history and a `stabilize_migrations` table, mg-up shall use `stabilize_history` and take over `stabilize_migrations` in place, without running migration SQL again and without moving any table.
- R5. If the target schema holds another project's migretti history and neither R3 nor R4 applies, then mg-up shall exit 1 before any change and name `--history-schema` and the move procedure.
- R6. The history schema shall differ from the target schema; if it does not, then mg-up shall exit 1 before any change.
- R7. mg-up shall be correct in both orders a host can run: stabilize first on an empty database then the host's migrations, and the host's migrations first then stabilize.
- R8. mg-status shall report from the history schema in use and create nothing.
- R9. When a database URL carries a connection parameter both in the URL itself (host, port, user, password, dbname) and as a query parameter, the CLI shall connect with the query parameter's value, as libpq and the runtime engine do.
- R10. If a stabilize migration file is marked `-- migrate: no-transaction`, then mg-up shall refuse a history schema run before any change (the schema redirect needs a transaction).

## Technical problems
1. Directing migretti's tracking tables to one schema and stabilize's DDL to another, with migretti unchanged.
2. Choosing the history location deterministically on every run, and on existing databases without operator action.
3. One URL resolving to two targets between the CLI and libpq.

## Solution domains
- PostgreSQL name resolution: unqualified `CREATE TABLE` and unqualified references resolve through `search_path`; `SET LOCAL` lasts until the end of the transaction (PostgreSQL manual, Schemas and SET).
- migretti 0.10.0 runs each migration's up SQL and then inserts into `_migrations` in the same transaction, and resolves `_migrations` through the connection's search_path.
- libpq connection URIs: query parameters are applied after the URI components.

## Alternatives
### Separating history from DDL
- Connection search_path = history schema only; each staged migration's up section is wrapped with `SET LOCAL search_path TO "<target>"` before and `SET LOCAL search_path TO "<history>"` after [CHOSEN]: migretti's ensure_schema, reads and its post-migration INSERT all resolve to the history schema, while the migration's own DDL lands in the target. Checksums are of the staged (wrapped) content, deterministic per (target, history) pair, and only stabilize's own runs verify them.
- search_path "history, target" without wrapping [REJECTED: unqualified CREATE TABLE goes to the first schema, so stabilize's tables would land in the history schema].
- search_path "target, history" [REJECTED: `_migrations` resolves to the host's table in the target].
- Move stabilize's tables to their own schema [REJECTED as the only path: impossible for hosts whose migrations depend on the tables in place; kept as documented guidance].
- Write rows into a differently named history table [REJECTED: migretti's table name is fixed; writing its tables directly bypasses migretti].
### URL precedence
- Query parameters win, as libpq [CHOSEN]: the runtime engine passes the URL to libpq unchanged, so the CLI must resolve the same target.
- Refuse when both are present [REJECTED: a valid libpq URL would stop working in the CLI only].
