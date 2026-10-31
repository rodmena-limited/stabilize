# #105 — 0.34.2: an unmigrated database is refused with "run stabilize mg-up"; the old-runner guard works on the real CLI

Ticket: issuedb #105. Found 2026-10-31 when the ten upgrade tests that had always skipped were run for the first time against published 0.33.1, after the director asked what the eleven skips in the 0.34.1 suite were.

## EARS SPEC
- If `stabilize_tenancy` does not exist in the schema (a database built by a release before 0.34.0, or never migrated), then every engine entry point shall raise `TenancyModeError` naming `stabilize mg-up`, instead of psycopg's "relation does not exist".
- The test suite's old-runner guard (`tests/mg_support.py::assert_own_runner`) shall obtain a published runner's version and import location from the runner's own interpreter, because the `stabilize` CLI has no `--version` flag in any release.
- The ten upgrade tests that depend on published runners shall run, not skip, when `STABILIZE_OLD_RUNNERS` provides 0.33.1 and 0.32.1; with `STABILIZE_REQUIRE_OLD_RUNNERS=1` a missing runner fails the test.
- When an engine component runs its first-use schema check on PostgreSQL, the engine shall read `pg_proc` for `stabilize_tenant_fence()` and `stabilize_is_table_owner(regclass)` and shall raise `EngineSchemaError`, naming the function and the remedy, if a body differs from the one mg-tenancy or the tenant_key migration installs, if the function is SECURITY DEFINER, or if it lacks `SET search_path = pg_catalog, pg_temp`. While the schema is in tenancy mode, both functions shall be required; in single-tenant mode the owner function shall be checked only when present. SQLite is out of scope (one process, one file, no host DDL).
- The expected owner body shall be a constant that a test compares with the migration file, and the expected fence body shall be read from the one source mg-tenancy installs, so the check and the installer cannot drift.
- When `stabilize --version` is run, the CLI shall print `stabilize <version>` and exit 0.
- The 0.34.2 release shall be gated by the full pytest suite on both backends with the old runners provided (zero skips from missing runners), the golden tests and the examples (director's rule of 2026-10-31).

## TECHNICAL PROBLEMS
1. Error mapping for an undefined mode table at the first statement of every transaction (SQLSTATE 42P01 raised inside `ARM_SQL`, before `check_mode_row` can speak).
2. A self-confirming harness: the guard's unit test planted a shim that answered `--version` while the real CLI never did, so the ten upgrade tests had never run and their skip hid it.

## SOLUTION DOMAINS
- The `sqlstate()` mapping already used for 42501 in `persistence/tenancy.py`, and the `TenancyModeError` wording `check_mode_row` already carries.
- The checkout's own rule: a check that cannot go green cannot go red. The shim now behaves like the published CLI (usage error on `--version`, exit 2) and a third test uses the real published runner.

## ALTERNATIVES
- Map 42P01 in `arm_connection` [CHOSEN: one place, every entry point, no extra round trip].
- Pre-check `to_regclass` before `ARM_SQL` [REJECTED: a second round trip per transaction].
- Keep `--version` in the guard and add the flag to the CLI [REJECTED: published releases never get the flag; the guard must work on them].

## Synthesis notes
- The engine already refused the unmigrated database; the defect was the message. A user who upgrades the code and starts a processor before `mg-up` now reads the instruction instead of a relation name.
- Red: `tests/test_mg_up_engine_tables.py::test_an_unmigrated_database_names_mg_up_from_the_first_statement` and `::test_task_leasing_on_an_old_database_names_mg_up_and_creates_nothing` fail on the 0.34.1 engine. Green after the one-clause change. `tests/test_gate_hygiene.py` guard tests: 3 fail on the old guard, pass on the new; `test_a_published_runner_passes_the_guard` runs the real 0.33.1.
- Old runners for this host live in `/tmp/claude-1000/oldrunners/<version>` (uv venvs of the published wheels); `STABILIZE_OLD_RUNNERS="0.33.1=…/bin/stabilize;0.32.1=…/bin/stabilize;"`.
- Function bodies: red without the comparison, 3 failed (rewritten fence, rewritten owner, SECURITY DEFINER fence) and 2 passed (constant equals the migration file; installed functions pass in both modes); green, 30 passed with the fence, mg-tenancy and tenant-schema tests. `--version`: 2 failed on the unchanged CLI, 2 passed after.

## Done (2026-10-31)
- Gate: full suite on both backends with the published runners provided, `-n 3`, no edits to the tree during the run: 2969 passed, 1 skipped (the SQLite-only demonstration, by design), 0 failed, 7 min 55 s. Golden tests and the headless examples are inside that run. An earlier run of the same suite was voided because the tree was edited while it ran (recorded in memory, not a product defect).
- Release 0.34.2 built from `git archive` of the release commit with `SOURCE_DATE_EPOCH`; digests and the commit recorded on the ticket after upload.
