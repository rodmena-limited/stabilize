# 95 — mg-up and mg-status hand off to migretti; existing databases are taken over in place

Ticket: issuedb #95. Reported by casebound-audit-d24037 (AgentBus thread
01M64Q3B9BG6FHPV96FSMGJJ5P, 2026-10-29), confirmed against v0.33.1 and main 7b6bb68.

## Finding

`stabilize mg-up` has never called migretti. Since `ffa33a7` (2025-12-23) it has
run its own apply loop: its own connection, its own tracking table
`stabilize_migrations` (MD5, keyed by filename), no lock, and a checksum mismatch
on an applied migration only warns unless `STABILIZE_STRICT_MIGRATIONS=1`.
`84281b7` (2026-01-05) converted the SQL to migretti format, added migretti to
the extras and switched the test suite to `mg apply`, but left `mg-up` alone. From
then on every PostgreSQL test built its schema with migretti while customers ran
the homegrown runner, which no schema test exercised.

## EARS spec

### Hand-off
- R1. `stabilize mg-up` shall apply stabilize's PostgreSQL migrations by running migretti's `apply` command and shall execute no migration SQL itself.
- R2. `stabilize mg-status` shall report migration state from migretti and shall create, alter or write no table.
- R3. stabilize shall run migretti in a new private directory (mode 0700) holding a `migrations/` folder with exactly the migration files shipped in the installed package, byte for byte, and no `mg.yaml`.
- R4. stabilize shall run migretti with the Python interpreter that runs stabilize, so that the migretti installed beside stabilize is the one used.
- R5. stabilize shall pass migretti the connection target and every libpq parameter the operator gave `mg-up` (including `sslmode`, `sslrootcert`, `sslcert`, `sslkey`, `connect_timeout`), through `MG_DATABASE_URL` and never on a command line.
- R6. stabilize shall remove `MG_ENV`, `MG_DB_HOST`, `MG_DB_PORT`, `MG_DB_USER`, `MG_DB_PASSWORD`, `MG_DB_NAME` and `MG_LOCK_ID` from migretti's environment. With `MG_DATABASE_URL` set, migretti 0.10.0 ignores the `MG_DB_*` fields; `MG_ENV` makes it fail (no `mg.yaml` in the staging directory) and `MG_LOCK_ID` changes its lock. They are removed so that no variable meant for another migretti project changes what stabilize's run does.
- R7. Where a target schema is given (`?schema=`, `MG_SCHEMA`, `schema:` in `mg.yaml`), stabilize shall create the schema if missing and migretti's tracking tables and every stabilize table shall be created in that schema.

### Takeover of databases set up by the old runner
- R8. When `mg-up` meets a target schema that holds `stabilize_migrations`, it shall record in migretti every migration the old runner applied, using only migretti's `apply` and `fix` commands, without running that migration's SQL again, before it applies any pending migration.
- R9. When the takeover has completed, `mg verify` against the staged files shall succeed and `mg status` shall report every migration the old runner applied as applied.
- R10. stabilize shall leave `stabilize_migrations` and its rows unchanged; it remains the record of what the old runner applied and when. An older release's `mg-up` run after the takeover shall exit 0 and apply nothing.
- R11. When `stabilize_migrations` names a retired migration (`01KDRK3TXW4R2GERC1WBCQYJGG_rag_embeddings.sql`, shipped in 0.9.2 to 0.12.1; `01M482Z7ZP5AXJ8PJCGB5XZTCZ_widen_queue_ids_to_bigint.sql`, never tagged), `mg-up` shall leave it out of migretti's history, say so in its output, and drop nothing.
- R12. If a `stabilize_migrations` row names a file stabilize never committed, or carries a checksum that no committed version of that file had, then `mg-up` shall exit 1 before changing anything and name the row.
- R13. If `mg-up` is interrupted at any point during the takeover, then the next `mg-up` shall finish it and reach the same end state as an uninterrupted run.
- R14. If migretti's history in the target schema holds a row for one of stabilize's migrations whose checksum is neither the current file's nor the takeover placeholder's, then `mg-up` shall exit 1 before changing anything.

### Integrity and concurrency
- R15. If `mg verify` fails against the staged files (an applied migration whose file changed, or an applied migration stabilize does not ship), then `mg-up` shall exit 1 before applying anything. This replaces the warn-only checksum check and `STABILIZE_STRICT_MIGRATIONS`.
- R16. If two or more `mg-up` runs start against one database at once, then they shall run one at a time and each shall exit 0 with the same end state. Tested at 4 concurrent runs.
- R17. If a run waits for another run longer than its lock timeout (default 900 s, `--lock-timeout SECONDS`), then it shall exit 1 without changing anything; once the other run has released, a new run shall proceed. Both directions are tested.
- R18. If migretti is not installed, then `mg-up` and `mg-status` shall exit 1 and name `pip install stabilize[postgres]`.
- R19. If migretti exits non-zero, then `mg-up` shall exit 1 and print migretti's output with any password redacted.
- R20. The output of `mg-up` and `mg-status` shall never contain the database password.

### Same defect class, found by the sweep (2026-10-29)
- R25. stabilize shall ship the PostgreSQL event store tables (`events`, `snapshots`, `event_subscriptions`, including `events.commit_xid` on PostgreSQL 13 or later) and `task_leases` as migretti migrations applied by `mg-up`, idempotent against tables that already exist.
- R26. On PostgreSQL, `TaskLeaseManager` shall issue no DDL; if `task_leases` is missing it shall refuse and name `stabilize mg-up`.
- R27. If `PostgresEventStore` refuses a database, then it shall name `stabilize mg-up` and release its hold on the connection pool before raising. Tested over 5 refused constructions with the suite's held-pool teardown check.
- R28. `PostgresEventStore(create_tables=True)` shall keep working and raise a `DeprecationWarning`.
- R29. `mg-check-grants` shall require grants on the 8 core engine tables only.

### Tests and evidence
- R21. The test suite shall build its PostgreSQL schema with `stabilize mg-up`, the command customers run, not with `mg apply`.
- R22. The upgrade tests shall set up their databases with the published stabilize 0.33.1 and 0.32.1 from PyPI, not with a model of the old runner.
- R23. Takeover of a 0.33.1 database shall complete within 60 s on a CI job (2048 MB, 4 CPU); the measured time is recorded on the ticket.
- R24. Every new test shall be observed failing on published 0.33.1 for its stated reason before the fix commit, and shall have a killing mutant or a manual proof in `audit/redproof`.

## Technical problems (in implementation order)
1. Delegating a migration run to an external CLI whose inputs are its working directory and its environment, without letting the caller's directory or environment leak in.
2. Adopting migration history recorded by another tool (baseline adoption) through the new tool's own interface, idempotently and resumably.
3. Mutual exclusion of concurrent migrators across processes and hosts, with a bounded wait.
4. Integrity of applied migrations (drift and foreign-history detection) before any change.
5. A test suite that exercises the path customers run, including upgrades from the real previous releases.

## Solution domains
- Migration tooling: migretti (house prescription, global CLAUDE.md). Its interface: `apply`, `fix --applied`, `verify`, `status`/`list`; library functions `get_connection`, `tracking_tables_exist`, `get_applied_migrations_details` (read-only). Config only from `./mg.yaml`, `./migrations/` and `MG_*` variables.
- Baseline adoption: the concept Flyway calls `baseline` and Django calls `--fake`: record a migration as applied without running it. migretti has no such command; its closest is `fix --applied`, which needs an existing row.
- Mutual exclusion: PostgreSQL session advisory locks with `lock_timeout` (PostgreSQL manual, explicit locking); codebase already uses `pg_advisory_xact_lock(class_id, hashtext(key))`.
- Process isolation: a subprocess with an explicit environment and working directory (codebase pattern for ShellTask).

## Alternatives

### How stabilize runs migretti
- Subprocess `python -m migretti <command>` with a private working directory and a scrubbed environment [CHOSEN]: `os.chdir` is process-wide, so the in-process alternative would move the working directory of every thread in a service that calls `mg_up()` from code.
- In-process calls to `migretti.core.apply_migrations()` after `os.chdir` [REJECTED: changes the working directory of the whole process; migretti reads `MG_*` from `os.environ`, so scrubbing would also alter the caller's environment].
- `mg` from `PATH` [REJECTED: may be a different migretti from the one installed with stabilize].

### Where migretti reads the files from
- A fresh temporary directory with the packaged files copied byte for byte [CHOSEN]: one code path for an installed wheel and a checkout, and no `mg.yaml` in it.
- The installed package directory [REJECTED: a checkout has `mg.yaml` at the repository root (localhost dev database), so the two layouts would behave differently].

### How existing databases are taken over
- Placeholder `apply` then `fix --applied` through migretti's CLI [CHOSEN]: stabilize stages a placeholder file (`SELECT 1`) under each adopted id, runs `mg apply` so migretti records the id, then runs `mg fix <id> --applied` from the real files so migretti records the real checksum. Only migretti writes migretti's tables. Resumable: a placeholder row is recognisable by its fixed checksum. Migretti's log shows UP (placeholder) then FIX for each adopted id; `stabilize_migrations` keeps the original times.
- stabilize inserts rows into `_migrations` and `_migrations_log` itself [REJECTED: writes another tool's tables behind its interface (global rule: never write a product's data directly) and couples stabilize to migretti's internal layout].
- Re-run every migration with `mg apply` and rely on idempotent SQL [REJECTED: `initial_schema` uses plain `CREATE TABLE`, and later migrations convert column types and data; re-running is neither possible nor safe].
- Keep writing `stabilize_migrations` alongside migretti forever [REJECTED: keeps the second tracking table this ticket exists to retire].
- Ask migretti for a baseline command [REJECTED by the operator, 2026-10-29: "migretti won't change anything"; "Stabilize cleans up its own mess"].

### Concurrency
- A stabilize session advisory lock held on its own connection for the whole run, with `lock_timeout` [CHOSEN]: the takeover is several migretti commands, and migretti's own lock covers one command at a time.
- migretti's lock alone [REJECTED: two runs can interleave between commands during a takeover].

### Integrity
- Foreign and failed history checked on every run from migretti's own read functions, before any change [CHOSEN, added during implementation 2026-10-29]: when no `stabilize_migrations` exists, `mg verify` alone would refuse a shared schema only with migretti's generic "no file on disk"; the explicit check names the ids and the `?schema=` remedy.
- `mg verify` before `mg apply`, failing closed [CHOSEN]: migretti's own check covers changed files and applied ids with no file (another project's history in the same schema, or a newer stabilize's).
- Keep the warn-only default [REJECTED: a changed applied migration is a tamper or drift signal; bank-grade requires refusing].

### Event store and task-lease tables
- Ship them as migretti migrations in stabilize's set [CHOSEN]: the house rule is migretti for all PostgreSQL migrations, and a runtime CREATE forces a standing CREATE grant on the runtime role.
- Keep runtime DDL and hand-run `setup_ddl()` [REJECTED: schema changes outside migretti, the defect this ticket exists for].
- A separate migretti set per feature [REJECTED: two histories per schema, which migretti's fixed `_migrations` table cannot hold].
- Require the event store tables in `mg-check-grants` [REJECTED: would turn every existing deployment's grant check red for a feature most do not use].

### A project that runs its own migretti migrations
- stabilize in its own schema with its own migretti history; same-schema mixing refused by `mg verify` [CHOSEN].
- Shared `_migrations` with the project's own set [REJECTED: the project's `mg verify` would fail on stabilize's ids and stabilize's on the project's; `mg rollback` could reach the other set].
