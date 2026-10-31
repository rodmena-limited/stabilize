# stabilize 0.34.0 — the multi-tenant release (ticket #103)

Plan of record: `/home/farshid/.claude/plans/joyful-dancing-balloon.md` v7 (11 work items, cross-cutting
rules 1-8, bug table B01-B65, decisions D1-D8). Each work item is red-before-green: the test runs and fails
on the current code before the fix is written. No gate until every item is done.

## EARS SPEC

### WI-1 hygiene (commit 5d4a919)
- The stabilize repository shall not track `mg.yaml`; the stabilize CLI shall read a YAML target file only when `--config PATH` is given.
- If `MG_DATABASE_URL` is set together with any of `MG_DB_HOST`, `MG_DB_PORT`, `MG_DB_USER`, `MG_DB_PASSWORD`, `MG_DB_NAME`, `PGHOST`, `PGPORT`, `PGDATABASE`, then the stabilize CLI shall exit 1 and name the variables that are set.
- The stabilize build shall pin `hatchling==1.33.0` and require `python-ulid>=3.2`; the pytest timeout of 600 s shall live in `pyproject.toml` only.
- When a probe cannot reach PostgreSQL, the probe shall exit 77 and `run_all.sh` shall count it SKIP; where `PROBE_REQUIRE_PG=1`, `run_all.sh` shall fail on any SKIP.
- The red-proof runner shall not wait on host load inside CI (`audit/redproof/host.py` removed).

### WI-2 tenant key and tenancy mode — slice 1 (column, mode record, API)
- The `01M67VCHWECA748FVXG88MF3YF_tenant_key` migration shall add `stabilize_tenant TEXT NOT NULL DEFAULT ''` to the 12 engine tables with `lock_timeout` 5 s, create `stabilize_tenancy (id SMALLINT PRIMARY KEY CHECK (id = 1), mode TEXT NOT NULL, enabled_at TIMESTAMPTZ, enabled_by TEXT)` seeded `(1, 'single')`, and create `stabilize_is_table_owner(regclass)` as STABLE, SECURITY INVOKER, `search_path = pg_catalog, pg_temp`.
- `stabilize_tenancy` shall be excluded from `engine_tables()`; `mg-check-grants` shall require SELECT on it and shall report its absence as a missing table.
- When a SQLite engine file (store, queue incl. custom queue tables and their `_dlq` siblings, event store, task leases, signals) is first opened by 0.34, stabilize shall add the tenant column to every engine-shaped table that lacks it and seed the mode record, in one `BEGIN IMMEDIATE` transaction.
- Every store, queue and event store constructor on both backends shall accept `tenant` matching `^[A-Za-z0-9.:-]{1,64}$` or `''`; if `tenant` is `None` or malformed, then the constructor shall raise `TenantError`.
- Constructing a `PostgresWorkflowStore` or `PostgresQueue` shall borrow no connection and run no `on_borrow` hook.
- When a PostgreSQL store or queue borrows a connection for engine work, it shall read the mode record on that connection (at most every 5 s per part, `ModeReader`) before the caller's statements; if the table is absent, policed by row-level security, unreadable by the role (42501) or has no row, then it shall raise `TenancyModeError` naming the remedy (`stabilize mg-up`, `DISABLE ROW LEVEL SECURITY`, `GRANT SELECT ON stabilize_tenancy TO "<role>"`, or the seed INSERT). The engine shall never default the mode.
- Every engine borrow of a `PostgresWorkflowStore` or `PostgresQueue`, including the module-level functions in `postgres/queries.py`, `postgres/operations.py` and `queue/dlq.py` and the `TaskLeaseManager`, shall pass through the owner's `_borrow()` (they receive an `EngineBorrower`, never the pool); `tests/test_tenant_api.py` calls every public operation under a refused tenant and expects `TenancyModeError` from each.
- When a `PostgresEventStore` is constructed, it shall verify its schema and read the mode record (it connected at construction before 0.34 and keeps doing so).
- `is_healthy()` shall answer reachability only and shall not read the mode record.
- While a schema is in single-tenant mode, constructing with a non-empty tenant shall raise `TenancyModeError`; while in tenancy mode, constructing with `''` shall raise `TenancyModeError`.
- If `QueueProcessor`, `Orchestrator` or `store.transaction(queue)` receives parts whose `tenant` attributes differ, then it shall raise `TenantError` before any other work.
- `PoolOptions` shall accept `tenant_setting` (matching `^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$`, never the `stabilize.` class) and `audit_key: bytes`; `PoolOptions.key()` shall cover every dataclass field.
- `PostgresEventStore(create_tables=True)` shall create the three event tables with the tenant column plus the mode table and its seed; `setup_ddl()` shall contain the same DDL; `PostgresEventStore` shall apply `with_schema` to its pool options.
- If the target schema of `mg-up` is named after the connecting role while `"$user"` is on the search_path, then `mg-up` shall refuse with `TakeoverRefusedError` before any change (B65).

### WI-2 slice 3: tenancy-mode CLI and tenant-scoped statements (done)
- `stabilize mg-tenancy status` shall print the mode, `enabled_at` and `enabled_by` of the target schema.
- `stabilize mg-tenancy enable` shall take mg-up's advisory lock, require every shipped migration applied, require zero rows in every engine table and in every custom queue table named by `pipeline_executions.queue_table`, and shall then set `stabilize_tenancy` to `(mode = 'tenancy', enabled_at = now(), enabled_by = current_user)` in one transaction. It shall print that every engine process must be stopped first (rule 2).
- `stabilize mg-tenancy disable` shall have the same preconditions and set the mode back to `single`.
- If any precondition fails, `mg-tenancy enable|disable` shall exit 1, name the failing table or migration, and change nothing.
- Every INSERT into an engine table shall set `stabilize_tenant` to the constructing part's tenant, on both backends.
- Every SELECT, UPDATE and DELETE on an engine table, including subqueries, CTE arms and the queue poll, shall carry `stabilize_tenant = <tenant>` as a bound parameter; module-level functions take the tenant from the `EngineBorrower` they receive.
- When an insert conflicts with a row the caller cannot see (another tenant's), the engine shall raise `TenantMismatchError`; a conflict with a visible row keeps its usual contention result (rule 4).
- While a schema is in tenancy mode, a part constructed for tenant A shall observe none of tenant B's rows through any public operation; `tests/test_tenant_isolation.py` enumerates the operations on PostgreSQL, and on SQLite through two processes sharing one file.
- The SQLite `ConnectionManager` shall refuse a second tenant on the same file within one process (`:memory:` exempt).
- The monitor shall accept `--tenant` and `--all-tenants`; while the schema is in tenancy mode, one of them shall be required.
- `stabilize mg-tenancy` shall accept a `sqlite:///` URL and switch the file's mode record under the same empty-file rule.
- B66 (found by this slice's live run): the PostgreSQL processed-message retention batch shall delete at most `batch_size` rows per statement and at most `max_rows` per call, whatever the plan and however `processed_at` ties (`tests/test_audit_retention.py::test_cleanup_never_deletes_past_max_rows_when_timestamps_tie`).
- Deferred from this slice to slice 4: the SQLite one-tenant-per-file refusal in the `ConnectionManager` (the in-process isolation tests use two tenants on one file today and move to two processes when the refusal lands).
- Slice 4 adds the GUC checks, the FORCE RLS refusal, the CHECK constraint, the fence, the default drop and the `TenancyModeChanged` latch.

### WI-2 slice 4: the PostgreSQL fence and the armed borrow path (done)
- Every engine borrow of a PostgreSQL store, queue or event store shall run, as its first statement, `SELECT set_config('stabilize.tenant', <tenant>, true) WHERE coalesce(current_setting('stabilize.tenant', true), '') = ''`; if that statement returns no row (the setting was already set on the connection), then the borrow shall raise `TenantContextError` naming the value found, and the connection shall go back to the pool.
- Where `PoolOptions.tenant_setting` is configured, the borrow shall refuse a current value of that setting outside `('', <tenant>)` with `TenantContextError`, and shall then set it transaction-locally to the tenant.
- When engine code commits or rolls back on a borrowed connection before the borrow ends, the connection shall re-arm both settings before the next statement (connection subclass; structural, not a list of call sites).
- If a borrowed connection is in autocommit mode, then the borrow shall refuse it with `TenantContextError`.
- `stabilize mg-tenancy enable` shall, in the one transaction that flips the mode, create the function `stabilize_tenant_fence()` (`SECURITY INVOKER`, `SET search_path = pg_catalog, pg_temp`) and a `BEFORE INSERT OR UPDATE OR DELETE FOR EACH ROW` trigger `stabilize_tenant_fence` on every engine table and every custom queue table, add the constraint `stabilize_tenant_nonempty CHECK (stabilize_tenant <> '')`, and drop the `''` default of `stabilize_tenant`.
- The fence shall raise SQLSTATE `ST001` when `current_setting('stabilize.tenant', true)` is null or `''`; `ST002` on INSERT when the setting differs from `NEW.stabilize_tenant`, on DELETE when it differs from `OLD.stabilize_tenant`, and on UPDATE when it differs from `OLD.stabilize_tenant` or `NEW.stabilize_tenant` differs from `OLD.stabilize_tenant`; and `ST003` when the setting is `'*'` and `pg_has_role(current_user, <table owner>, 'MEMBER')` is false. A setting of `'*'` held by an owner member passes.
- `stabilize mg-tenancy disable` shall drop the triggers, the function and the CHECK constraint and restore the `''` default in the same transaction as the mode flip.
- If any engine table has `relforcerowsecurity` set, then `mg-tenancy enable` shall refuse and name the tables and their policies.
- If `stabilize.tenant` is set for the database or any role (`pg_db_role_setting`), or is non-empty on a fresh connection, then `mg-tenancy enable` shall refuse and name where it is set.
- The engine shall map errors by SQLSTATE through one `sqlstate(exc)` helper: `ST001` to `TenancyModeChanged`, `ST002` to `TenantMismatchError`, `ST003` to `TenantPermissionError`; `23502` and `23514` map to `TenancyModeChanged` only when `diag.column_name` is `stabilize_tenant` or `diag.constraint_name` is `stabilize_tenant_nonempty`.
- The arm shall be one statement that also returns the mode record and its row-security flags, and the host setting's state when `tenant_setting` is configured; it shall run lazily before the first statement of a borrow and before the first statement after a commit or rollback inside the borrow (a commit that ends a borrow costs nothing).
- While the mode last read is `single`, is younger than 5 s and no `tenant_setting` is configured, the borrow shall run no arm (there is no fence to satisfy and the setting is unused); the mode is re-read, through the same statement, at most every 5 s. Measured cost: single-tenant mode unchanged within one statement per 5 s; tenancy mode one statement per engine transaction (one stage went from 87 to 135 statements in the budget test before the skip).
- Rule 6's "first statement of every engine transaction" is therefore read as "before the first statement, in tenancy mode or whenever a host setting is configured"; the decision is recorded here because the budget test showed a 55% statement increase for single-tenant installs that gain nothing from it.
- Slice 5 adds the `TenancyModeChanged` latch on the processor, the SQLite triggers and UDF, and the SQLite one-tenant-per-file refusal; slice 6 adds the first-use schema check, the indexes and the namespaced keys.

### WI-2 remaining slices (to be specified as each starts)
Statements on every SQL site (rule 1 explicit column lists), the trigger fence and `mg-tenancy enable|disable|status`, the `TenancyModeChanged` latch, per-instance recorder/deduplicator, namespaced subscription ids and locks, the schema check, indexes, SQLite triggers, runbooks.

### WI-3 … WI-11
Specified when each item starts; the plan file holds the design.

### WI-2 slice 2: rule 1 column lists (done)
- `stabilize.persistence.columns` shall hold one column tuple per engine table, ending in `stabilize_tenant`; `columns(table, backend)` shall equal the catalog of that table on that backend (set equality; PostgreSQL `events` adds `commit_xid`, SQLite `queue_messages` adds `version`), checked live on both backends.
- Every engine SELECT shall name its columns from that module; no `SELECT *`, `RETURNING *` or `alias.*` shall remain on an engine table (`tests/test_engine_columns.py::test_no_engine_statement_selects_star`; 52 sites converted in 17 files, the monitor included).
- The PostgreSQL row converters shall index every column they read strictly (`row["col"]`), so a row missing a listed column raises `KeyError` naming it; the columns a converter does not read are named in the test: `pipeline_executions` {created_at, stabilize_tenant}, `stage_executions` {execution_id, stabilize_tenant}, `task_executions` {stage_id, stabilize_tenant}. The tenant column leaves those sets when `Workflow.tenant` lands.
- The PostgreSQL event converter shall build its row by name from the column list, never by position.

## TECHNICAL PROBLEMS
1. Row ownership under one shared schema: a tenant key every engine row carries and every statement filters on.
2. A mode the engine can trust: where the schema records single vs tenancy, how the engine reads it without a policy in the way, and what it refuses.
3. Construction-time agreement: parts (store, queue, event store, processor) built with different tenants must not work together.
4. Hosts that upgrade code before schema: a clear refusal that names `stabilize mg-up`, never a silent default.

## SOLUTION DOMAINS
- Multi-tenant row keys -> PostgreSQL RLS manual (`current_setting`, `set_config(..., true)`), Casebound's and Provenance's existing policies; engine column `stabilize_tenant` so a host policy can reference it.
- Mode record -> singleton table with `CHECK (id = 1)` (SQL standard); un-policed by contract so a plain SELECT answers under any role.
- Constructor contracts -> existing stabilize pattern (`release_pool_once`, `with_schema`), `StabilizeError` hierarchy.
- Migrations -> migretti (house tool); additive column with a constant default is metadata-only on PostgreSQL 11+.

## ALTERNATIVES
- Tenant key: engine column `stabilize_tenant` [CHOSEN] vs schema-per-tenant [REJECTED: Casebound and Provenance run RLS in one schema; a schema per tenant multiplies migrations and pools] vs host-owned column only [REJECTED: the engine cannot filter on a column it does not know].
- Mode storage: singleton table [CHOSEN] vs a GUC or `ALTER DATABASE` setting [REJECTED: not visible to a plain SELECT on SQLite, not transactional, not in the migration history] vs per-process flag [REJECTED: two processes could disagree about one schema].
- Mode default when the row is missing: refuse [CHOSEN] vs default to single [REJECTED: a deleted or unreadable row would silently admit cross-tenant reads].
- Enabling tenancy: only on an empty schema (D2) [CHOSEN] vs live conversion with backfill [REJECTED for 0.34.0: no owner for rows written before the mode flipped; a later release].
- Mode read cadence: throttled re-read every 5 s per part on the borrowed connection [CHOSEN] vs once at construction [REJECTED: a mode flip under a running process would go unseen] vs every transaction [REJECTED: one extra round trip per transaction].
- Where the first mode read happens: at first engine borrow [CHOSEN, 2026-10-31] vs in the constructor [REJECTED after the live run: a constructor borrow runs the host's `on_borrow` hook outside any request (Casebound's hook reads a request contextvar and would raise), breaks `is_healthy()` on an unreachable database, and failed 9 pinned tests (`test_queue_pool_holder`, `test_pool_hooks`, `test_pool_options_and_lifetime`)]. The processor still fails fast: `QueueProcessor.__init__` hydrates its deduplicator through the store, which is a borrow.
