# SPEC 11 — Document hybrid-schema deployment: scope search_path, never apply it globally

- **Ticket:** issuedb #11
- **Status:** closed (verified)
- **Origin:** agent-mail `verify-result` from sponsorsignal, id
  `01KYTJ9R7EAM1PV9KP14HKNG8K`, thread `thr-7330521db18943b6b93d`, replying to the
  0.20.0 `mg-up` schema-targeting fix (issuedb #2 / SPEC 2).

## Context

sponsorsignal confirmed the 0.20.0 schema-targeting fix on their test database, and
in doing so reported a defect in the migration guidance that shipped with it: our
runtime advice was read as "point the DSN at `options=-csearch_path=stabilize`",
which is correct only for a Stabilize-only database.

Their deployment is hybrid — application tables in `public` (managed by migretti),
Stabilize tables in `stabilize`. Their application register already has a
`snapshots` table, and the Stabilize event store has its own `snapshots` table.
Applying `search_path=stabilize` to the shared global DSN would silently re-resolve
every unqualified `SELECT ... FROM snapshots` in their API to the event store's
table. Nothing raises; the query returns the wrong rows. They worked around it by
scoping only the Stabilize runtime connections and schema-qualifying admin queries
over the unscoped pool.

The guidance is not wrong for the single-tenant-database case, but it is unsafe as
written for a hybrid install, and a consumer following it literally gets silent data
corruption rather than an error. That belongs in the documentation, not only in one
peer's workaround.

## EARS requirements

- The PostgreSQL persistence guide shall state that the `search_path` option scopes
  only the connections Stabilize owns (workflow store, queue, event store) and shall
  not be applied to a database URL shared with non-Stabilize application code.
- Where a deployment keeps application tables in one schema and Stabilize tables in
  another (a hybrid install), the persistence guide shall warn that a globally
  applied `search_path` can silently re-resolve unqualified application queries to
  same-named Stabilize tables.
- The persistence guide shall name the concrete collision reported by sponsorsignal
  (an application `snapshots` table versus the event store's `snapshots` table) as
  the worked example of that failure.
- The persistence guide shall state that administrative or housekeeping queries that
  read Stabilize tables over an unscoped pool must schema-qualify those table names.

## Verification

The rendered `docs/guide/persistence.rst` PostgreSQL section is read back and each
"shall" line checked against its text; the outcome is recorded on issuedb #11 and
reported to sponsorsignal on thread `thr-7330521db18943b6b93d`.
