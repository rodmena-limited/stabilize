# 98 — mg-up adopts a schema that has stabilize's tables but no migration history

Ticket: issuedb #98. Found 2026-10-30 from ci-conductor's description of its schema.

## EARS spec
- R1. When the target schema holds stabilize's engine tables and neither `stabilize_migrations` nor any stabilize id in migretti's history, mg-up shall find from the catalog the lowest level k such that every table, column (with type), index and sequence type the first k migrations leave exists, and no object of a later migration exists unless that migration creates it with IF NOT EXISTS. It shall record the first k in migretti without running their SQL, re-run their idempotent non-catalog statements (the `setval` in 01M4PAJCF82KCKYT5X4GMZH2XG), and apply the rest with `mg apply`.
- R2. If the catalog shows objects of a migration beyond that prefix (a later migration partly or wholly present while an earlier one is not), or a migration only partly present, then mg-up shall exit 1 before any change and name each missing and each unexpected object.
- R3. The catalog check for each migration shall be derived from that migration's own SQL. A test shall show, for every k from 1 to the number of shipped migrations, that a schema holding exactly the objects of the first k files and no history is adopted by mg-up, after which migretti reports every shipped migration applied and `mg verify` passes; and that a schema with a column of the wrong type, or with a later column type change already present, is refused before any change.
- R4. All shipped migrations are DDL only; if a shipped migration contains INSERT, UPDATE or DELETE, then the adoption shall refuse to treat it as catalog-visible.
- R5. Adoption shall be idempotent and resumable like the #95 takeover, and shall be reported in mg-up's output with the level it found.

## Technical problem
Baseline adoption of an untracked schema: inferring applied migrations from catalog state.

## Solution domains
- PostgreSQL catalog (pg_class, pg_attribute, pg_index via to_regclass and information_schema), parsed against each migration's DDL.
- migretti adoption through its own commands (placeholder `mg apply`, then `mg fix --applied`), as in #95.

## Alternatives
- Expected objects parsed from each migration's SQL, checked in the catalog [CHOSEN]: one source of truth; a new migration needs no hand-kept table.
- A hand-written marker table per migration [REJECTED: drifts from the SQL; nothing forces it to be updated].
- Refuse with a message only [REJECTED by the operator's "implement all features"].
