# Changelog

## [0.30.4] - 2026-09-24

### Security

- **An upstream error could carry our own API key into errors, logs and
  stored task state (#59).** When an LLM endpoint or Highway answered with an
  error, `stabilize.llm` and the Highway task copied the response body
  verbatim: into `LLMError`, into an ERROR log line, and into the terminal
  task error that is persisted with the stage. A gateway that reflects the
  request's `Authorization` header therefore put our key in all three places.
  Reported by vellum-build-d8bbd2, who found it reaching a screen shown to
  council officers.
  - Upstream error text now has every credential we sent removed wherever it
    appears. Credential-shaped fields (`authorization`, `x-api-key`,
    `api_key`, `access_token`, `secret`) are masked whoever they belong to,
    DSN passwords are redacted, and the text is capped at 500 characters.
  - The upstream's own diagnostic (`unauthorized`, `model ... not found`) is
    kept.
  - `HTTPTask` is unchanged: its error is `HTTP <status>`, and the response
    body is a stage output by design.

## [0.30.3] - 2026-09-24

### Security

- **Most mistyped connection strings still echoed the password in 0.30.2
  (#58).** A connection string with a mistyped separator
  (`postgresql:u:<pw>@h/d`, `postgresql//u:<pw>@h/d`, or a bare
  `u:<pw>@h/d`) was quoted back by libpq in full. It contains no `://`, so
  neither the URL redactor nor 0.30.2's fragment scrub recognised it.
  - Of 446 generated malformed shapes whose raw libpq error contains the
    password, 325 still leaked on 0.30.2.
  - Any fragment of the input that libpq quotes back is now rendered from an
    allow-list: a bare word, the URL scheme, and a host with no spaces or query
    string are shown; everything else becomes `***`.
  - All 446 generated cases pass, and diagnostics such as
    `invalid connection option "HOST"` and the `+psycopg` scheme hint remain
    readable.
  - 0.30.0 to 0.30.2 each fixed the shapes I had thought of and missed others.
    The allow-list stops depending on that: it decides what may be shown, not
    what must be hidden.

- **A queue message that failed its field contract carried the rejected
  value in its traceback.** A free-form field (`signal_data`, `jump_context`,
  `instance_context`) sent with the wrong type raised `MessageContractError`
  chained to pydantic's `ValidationError`, which includes `input_value=<raw
  value>`. The processor logs failed messages with their traceback. The error
  already names the field and the reason, so the chain is now dropped.
  Reported as a class by trace-thinkpad-83589d (pydantic echoes refused input).

## [0.30.2] - 2026-09-24

### Security

- **A fragment of a password could survive redaction (#57).** In a
  keyword-form connection string with an unquoted space in the password
  (`password=abc def`), libpq quotes back the part after the space:
  `missing "=" after "def"`. 0.30.1's redaction rewrote URL userinfo and
  `password=<token>`, and the echoed fragment is neither, so it passed
  through.
  - Now the password is taken from the input string (URL userinfo, the
    `password=` value, and each whitespace-separated piece of it) and scrubbed
    from the error before anything else sees it. A diagnostic with no password
    in it, such as `invalid connection option "HOST"`, is kept.
  - The trigger is a connection string libpq cannot parse. resilient-circuit's
    environment-variable factory builds `password=...` unquoted, which
    produces exactly this string when a password contains a space.

## [0.30.1] - 2026-09-24

### Security

- **Circuit-breaker storage logged a malformed DSN's password in
  stabilize's own ERROR line (#56).** `RunTaskHandler` always builds circuit
  breaker storage from the database URL. When libpq could not parse that URL,
  the psycopg error was formatted into
  `PostgreSQL circuit breaker storage unavailable (ProgrammingError: ... u:<password>@...)`.
  - With `STABILIZE_CIRCUIT_STORAGE_STRICT=1`, the raised error and its chain
    carried the password instead.
  - resilient-circuit logged it as well: 0.7.0 as "Failed to ensure table
    exists", 0.8.2 and 0.8.3 as "Failed to verify circuit breaker schema".
  - A `+psycopg` suffix was stripped first, so that one form was spared;
    `+asyncpg`, and any other string libpq cannot parse, was not.
  - Now the URL is parsed before it reaches resilient-circuit, the failure is
    reported with `u:***@`, and there is no exception chain. resilient-circuit
    never receives the string, so this also protects anyone still on
    resilient-circuit below 0.8.4.
- **Correction:** the 0.29.1 announcement said a sweep found no stabilize code
  that logs a DSN. The sweep matched `logger(..., exc)` and missed an exception
  formatted into an f-string argument, which is exactly how this path did it.

### Changed

- An upper-case URL scheme (`POSTGRESQL://`) for circuit-breaker storage is
  lower-cased before use. libpq rejects the upper-case form, so it previously
  reached resilient-circuit only to fail.

## [0.30.0] - 2026-09-24

### Security

- **A PostgreSQL DSN that libpq cannot parse is refused before any pool
  exists, so its password no longer reaches your logs (#53).** Before, the
  malformed string went to psycopg_pool, which retried and logged libpq's
  parse error at WARNING on every attempt. libpq quotes the unparsed input
  back, password included. Measured with a `postgresql+psycopg://` DSN: 14 of
  28 log records carried the password, and the caller got a clean PoolTimeout
  after the acquire timeout, so an audit of your own except-blocks found
  nothing. Now `get_postgres_pool` raises at once:
  `ValueError: PostgreSQL connection string could not be parsed: ... u:***@...`
  with no exception chain. No pool is created and nothing is logged.
- **`resilient-circuit>=0.8.4`**, raised from 0.8.2. 0.8.2 and 0.8.3 log a
  malformed conninfo with its password.

### Added

- **`stabilize mg-check-grants --role ROLE [--db-url URL]` (#44).** Reports
  which engine tables the role cannot SELECT, INSERT, UPDATE or DELETE, a
  missing USAGE on the schema, and a missing USAGE on each table's serial
  sequence (INSERT needs it).
  - The table set is read from the migrations shipped in the package, so it
    follows new tables without anyone updating a list.
  - Exit codes: 0 when complete, 1 when anything is missing, 2 when the role
    does not exist or the check cannot confirm a table owner's own access.
  - Run it at deploy time as a user that can read the catalogue.
  - Not covered: the event-store tables and `task_leases`. The engine creates
    those at runtime, not through the migrations.
- **`stabilize[c]` (#45).** Installs `psycopg[c]`, the compiled driver linked
  against the system libpq. It needs `pg_config` and a compiler at install
  time, so it is not part of `all`. No extra declares `psycopg[binary]`.

### Changed

- **`mg-up` and `mg-status` state which driver will open the connection (#47).**
  They print the psycopg version, its implementation, the libpq version, and
  whether that libpq is the system one or bundled inside psycopg-binary. If
  that cannot be determined, they refuse to connect.
- **An import error is no longer reported as "psycopg not installed" (#47).**
  The install hint now appears only when psycopg itself is absent. Any other
  ImportError, such as a `psycopg_c` whose version does not match the
  installed psycopg, propagates with its own message. Before, it was reported
  as "psycopg not installed", on a host where psycopg was installed.
- **Database errors printed by the CLI are redacted.**
- **Fewer database round trips per stage (#46).** A one-stage workflow on
  PostgreSQL now issues 97 SQL statements instead of 110, and each additional
  stage 87 instead of 97.
  - Each message was marked processed twice: once inside the handler's
    transaction and once by the processor afterwards. The processor now skips
    its mark only when a committed transaction on the same thread already
    marked that exact message. A handler that does not mark still gets the
    processor's mark.
  - `process_all` counted the queue before every message. It now counts only
    when no message was ready, and still stops only when the queue is empty.
  - `tests/test_query_budget.py` fails if either number rises.
    `docs/guide/query_budget.rst` has the per-statement breakdown and what is
    left (#54).

## [0.29.1] - 2026-09-23

### Fixed — PostgreSQL connection pools leaked in 0.25.0 through 0.29.0 (#51)

- **`PostgresQueue` took one pool hold per operation and released one on
  `close()`, so its pool was never closed.** `_get_pool()` called
  `get_postgres_pool()` at 14 sites, and every call registered a holder. After
  N operations a queue needed N `close()` calls to release its pool. No caller
  did that, and no caller could have known to.

  With a constant DSN the leak is bounded at one extra pool per process: 5
  connections, up to 15. With a DSN that varies per tenant, for example
  `options=-c search_path=<tenant>`, every tenant gets its own pool, each of those
  pools leaks, and connections grow with the number of tenants. One platform
  hit its role's `connlimit` of 40 and was down for three days. The warning
  psycopg_pool printed was read as noise. **Read it:** "the pool was deleted
  while still open" means a pool was never closed.

  Introduced by 3bfa497 (0.25.0), which added holder-counted pool lifetimes.
  **Affected: 0.25.0 – 0.29.0. 0.24.x and earlier are not affected.**

- **`close()` released holds by DSN, not by pool.** `close_postgres_pool(dsn)`
  took one hold off **every** pool on that DSN. Closing a queue could therefore
  close a store's pool that had different options. On 0.29.0 this happened even
  when the queue had never performed an operation. A second `close()` took a
  hold that belonged to another owner.

  Now `PostgresQueue`, `PostgresWorkflowStore` and `PostgresEventStore` each
  acquire their pool once, at construction. `close()` releases exactly that
  pool's hold, exactly once per owner, through the new
  `ConnectionManager.release_postgres_pool(pool)`. A repeated `close()` does
  nothing. `close_postgres_pool(dsn)` is unchanged, for callers that already
  depend on it.

  Verified against a real PostgreSQL: on 0.29.0, holders went from 5 to 6 over
  15 operations, and one `close()` left the pool open. On this release the
  count stays at 1, and one `close()` closes the pool. `tests/test_queue_pool_holder.py`:
  7 of 9 fail against 0.29.0 (the 2 that pass are controls), and all 9 pass
  here.

### Upgrade notes

- **Go straight from ≤0.24.x to 0.29.1.** Any version from 0.25.0 to 0.29.0
  has the leak.
- **Pools have been keyed on DSN + pool options since 0.25.0.** This release
  does not change that. A pool taken with an explicit
  `get_postgres_pool(dsn, min_size=…, max_size=…)` is a different pool from the
  default one shared by `PostgresQueue(dsn)` and `PostgresWorkflowStore(dsn)`.
  On 0.24.x and earlier they were the same pool. Budget up to `max_size`
  connections (default 15) per distinct key, per process. Release an explicit
  hold with `get_connection_manager().release_postgres_pool(pool)`.
- **Correction to an advisory sent for this defect.** It said that 0.21.1 to
  0.25.2 discard URL query parameters. Only the CLI did: `mg-up` and `mg-status`
  in 0.25.2 and earlier rebuilt the conninfo string and dropped `sslmode`,
  `sslcert`, `sslkey` and the other query parameters (fixed in 0.26.0). The
  runtime engine has always passed the DSN to libpq unchanged, in every version.

## [0.29.0] - 2026-09-20

### Upgrade notes — observable behaviour change

- **A stage whose planning fails now reports failure instead of success (#50).**
  This is the one item here that can change what an existing pipeline reports.

  When planning a stage raised — a `StageDefinitionBuilder` that throws, an
  unknown reducer name, an ancestor-output failure — the engine recorded the
  error in `stage.context["exception"]`, set
  `context["beforeStagePlanningFailed"] = True`, and then completed the stage as
  **SUCCEEDED**. The flag was written at one site and read by none, and
  `determine_status()` returns SUCCEEDED for a RUNNING stage with no tasks.

  So the workflow reported success having run nothing, with the real error
  sitting in context where nothing looked.

  `determine_status()` now consults that marker and classifies through the
  existing `failure_status()`, so `continuePipelineOnFailure` and `failPipeline`
  are honoured as they are for any other failure.

  **What this means for you:** a pipeline that has been failing to plan and
  reporting green will now report red. The failure was always there; only the
  reporting changes. If a workflow turns red on this upgrade, read
  `stage.context["exception"]` — the cause has been recorded all along.

  This also corrects a claim made in the 0.28.x notes: an unknown reducer name
  was said to fail the workflow at runtime. It did not. It succeeded silently.

### Fixed

- **Signal storage latched off permanently after one transient fault (#48).**
  Shipped in 0.28.2. `supports_signal_storage()` cached a negative answer with
  no expiry, so a pool blip, a momentary permission change during a migration or
  a brief network stall degraded signal storage for the **life of the worker
  process** — reintroducing the unbounded `stage_executions.context` growth that
  issue 15 capped, with the warning firing once.

  A negative answer now expires after a 30s cooldown and the table is re-probed;
  recovery is logged at INFO. The guard blocks *and* releases.

  The probe that covered this tested only the block direction: it used two
  different roles, which reads as "both directions" and is not. It now takes one
  store through revoke → unusable → restore → **resumes**.

- **An unregistered stage type silently produced a no-op stage (#49).** A stage
  whose type has no registered `StageDefinitionBuilder` fell back to
  `NoOpStageBuilder`, so a caller who wrote a builder and forgot
  `register_builder()` got zero tasks and a SUCCEEDED workflow.

  `stage.type` is a free-form label and a multi-instance parent is legitimately
  taskless, so refusing unregistered types is not available — it would break
  essentially every workflow. Instead, following `STABILIZE_MERGE_STRICT`'s
  shape: the engine now **warns**, naming the stage, the type and the remedy.
  Set `STABILIZE_STRICT_STAGE_TYPES=1` to make it an error. Declared
  coordinators (`mi_config`) are exempt.

- **`examples/benchmark-overhead.py` could never pass.** It targets its own
  loopback server, which `HTTPTask`'s SSRF guard blocks by design. It now opts
  in with `allow_private_urls`, which is what a caller targeting a private
  address is supposed to do. The engine was correct; the example was not.

### Changed

- `audit/evaluations/run_all.sh` distinguishes **skipped** from **failed**. A
  probe needing a PostgreSQL container on a machine without Docker reported
  identically to a probe that failed.
- New `make probes-sqlite` / `probes-postgres` / `probes`, and `make check` now
  runs the Docker-free probe set. The probe suite — this repo's audit evidence —
  was previously run by no target at all.
- `make check` records a gate stamp keyed to a fingerprint of the source tree,
  and `scripts/release.py` skips its test stage when that stamp matches. A skip
  now rests on evidence that these exact bytes passed, rather than on
  `--skip-tests` asserting it. The stamp is refused if the tree changes while
  the gate runs.


## [0.28.2] - 2026-09-20

### Fixed

- **0.28.1 broke every deployment whose runtime role has per-table grants.**
  #16 made `set_stage_status` call `pending_signal_count` on every stage
  completion, which reads `workflow_signals`. A role granted privileges on the
  previously-known table set has none on that table, because nothing referenced
  it before 0.28.1, so the workflow died mid-flight with
  `InsufficientPrivilege` -- at stage completion, not at startup.

  `supports_signal_storage()` returned `True` on the strength of the backend
  class. The caller needs an answer about the connection: can this session reach
  that table. Different layers, same `True`. Support is now established by using
  the table once and is withdrawn when it cannot be used, so an unreachable
  table degrades to the pre-0.28.1 stage-context buffer instead of failing the
  workflow.

  The warning names where the data went, not merely that a fallback happened:
  a deployment that protects `workflow_signals` with row-level security while
  leaving `stage_executions` unprotected moves signal payloads outside that
  boundary, and "falling back to stage-context buffering" does not say so.

- **A signal could be dropped with no error.** `supports_signal_storage()` is
  checked before the write, so an unreachable table correctly takes the context
  path -- but if the check said yes and the write then failed, the caller had
  already committed to the store branch and the signal was lost silently.
  `buffer_signal` returns the row id, the caller checks it, and a zero falls
  through to the context buffer. Replacing a crash with a quiet data-loss path
  would have been worse than the crash.

- **A redelivered signal buffered twice.** The store-backed branch never called
  `mark_message_processed`; the context branch did. Now both do.

- **`mg-up` found no migrations from a source checkout (#42).** `get_migrations`
  resolved only through `importlib`, but the `.sql` files live at the repository
  root and are force-included into the wheel at build time, so a checkout found
  zero and reported "No migrations found in package" -- which reads as "this
  version ships none". It now falls back to the repository root, and when
  neither location has them, names both.

  The suite could not have caught this: `tests/test_cli_schema.py` monkeypatches
  `get_migrations` with a working replacement in three places.

### Changed

- The probe runner derives its list from the directory instead of carrying a
  hand-maintained copy. Four probes sat outside it and were counted as coverage,
  including the one for #16 and the only one that runs as a least-privilege role
  -- which is what caught the regression above.


## [0.28.1] - 2026-09-20

### Fixed

- **Schema verification asserted existence, not resolution (#43).** SHIPPED IN
  0.28.0 and reproduced against that published wheel. `_verify_schema` queried
  `information_schema` with no schema filter, so it was satisfied by a table in
  a schema the engine would never touch — a check that passes while resolving
  elsewhere, which is worse than no check because it converts absent protection
  into false confidence.

  Getting it right took four steps, each of the first three looking complete:

  1. `information_schema`, no schema filter — what 0.28.0 shipped
  2. `to_regclass` — right question, but only "resolves to anything"
  3. namespace compared against `schema=` — fires only when `schema=` is set,
     and a DSN `options` segment beats `schema=`, so callers following our own
     documented advice set no schema and got no protection
  4. with no schema configured, all tables must resolve to ONE namespace; a
     shadow of a single table splits them and is refused, naming which

  A fifth case is a **boundary, not a defect**, and is documented as one: a
  shadow of ALL the tables resolves consistently and passes — measured,
  `resolved_schema()` returning `'public'` while the real tables sit elsewhere.
  No engine-side check can close that without a configured expectation, so
  `PostgresEventStore.resolved_schema()` is exposed and its docstring states
  plainly that reading it is the only cover.

  Note for anyone writing a similar check: `to_regclass(name)::text` renders
  UNQUALIFIED exactly when the schema is on the search_path — the healthy case —
  so asserting a qualified name fails when everything is correct, and the
  obvious repair then accepts a shadow in any schema. Assert the namespace from
  `pg_class`/`pg_namespace`, never a rendered name.

- **Persistent signals move out of stage context into `workflow_signals` (#16).**
  Buffering appended to `stage_executions.context["_buffered_signals"]`, the
  structure issue 15 had to cap at 1000 entries because it grew unbounded, and
  which every consumer reading that column can see. `create_signals_table` and
  the buffer/consume/pending helpers existed and had **no callers at all**, so
  the table was created by migration on PostgreSQL and did not exist on SQLite.

  `WorkflowStore` gains `buffer_signal`, `consume_signal`,
  `pending_signal_count`, `discard_signals` and `supports_signal_storage`, all
  concrete with `0`/`None`/`False` defaults rather than abstract, so an
  out-of-tree store keeps importing and falls back to the context buffer.

  Consumption **dual-reads, context first**: a signal buffered before this
  change drains rather than sitting behind everything written since. Nothing
  rewrites rows that still carry `_buffered_signals`.

  PostgreSQL consumption is a single statement — `UPDATE ... WHERE id = (SELECT
  ... FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING` — so two workers racing on one
  suspended stage cannot both claim a signal, and a crash between claim and read
  cannot consume one without delivering it.

### Consumer impact

- `context->'_buffered_signals'` stops appearing on NEW rows. Existing rows keep
  theirs and nothing rewrites them, so a query reading that key by name will
  quietly return null for new rows while continuing to work for old ones —
  the shape that produces a wrong conclusion rather than an error.


## [0.28.0] - 2026-09-20

Nine defects, every one reproduced against the PUBLISHED 0.27.0 artifact from
PyPI before being fixed, and each carrying a probe in `audit/evaluations/` shown
to fail on 0.27.0 and pass here. Six were found by consumers asking questions
next to the thing rather than by an audit of this repo.

### Upgrade notes — observable behaviour changes

- **`PostgresEventStore(dsn)` no longer creates its own tables.** `create_tables`
  now defaults to **False**. A store constructed against a schema that is absent
  or out of date raises `EventStoreSchemaError` at construction, naming the
  missing object and printing the exact, schema-qualified DDL to apply. Pass
  `create_tables=True` to restore the old behaviour, or apply `setup_ddl()` as
  the migration operator.

  Why: a library that issues DDL from a constructor forces every consumer to
  grant its runtime role a standing CREATE privilege, and that grant then
  consents to whatever DDL a later release decides to run. On 0.27.0 the
  constructor also ran `ALTER TABLE events ADD COLUMN commit_xid` and a
  non-`CONCURRENTLY` `CREATE INDEX`, which takes ACCESS EXCLUSIVE on the events
  table.

- **`STABILIZE_TASK_LEASE` now fails closed.** When the flag is set and the lease
  manager cannot be initialised, the engine raises `TaskLeaseUnavailableError`
  instead of logging a warning and continuing. Previously a bare
  `except Exception` disabled leasing silently — including on a typo in
  `STABILIZE_TASK_LEASE_TTL_SECONDS` — so an operator who asked for
  single-execution across processes did not have it and had no way to tell.

- **A malformed queue message is now rejected at the boundary.**
  `create_message_from_dict` validates fields against their declared types and
  raises `MessageContractError` naming the field. Previously it was a bare
  `message_class(**data)` splat: `attempts="three"` constructed silently and
  raised `TypeError` far away in the retry path, reaching the DLQ attributed to
  whatever code touched it. Caller-owned payload (`context`, `outputs`) is
  deliberately unvalidated.

- **`cancel_remaining=True` now raises `NotImplementedError`** rather than being
  accepted and ignored. It is not implementable in this engine: every instance of
  a fixed multi-instance stage is dispatched when the parent completes, so by the
  time an N-of-M threshold is reached the remaining instances are already
  RUNNING, and there is no cancellation channel into a running task. Measured at
  count=5 and count=20; an admission-time check was also tried and is reached for
  every instance before any completes.

- **`mg-up` and `mg-status` now print the configuration source** and the
  redacted resolved target before connecting. `mg.yaml` is a filename migretti
  owns and reads from the working directory too, so a bare `stabilize mg-up` in
  a repo configured for migretti silently adopted that file.

### Fixed

- **`create_dynamic(initial_count > 0)` cleared `allow_dynamic` (#35).** It
  delegated to `create_fixed`, which replaces `mi_config` wholesale and knows
  nothing about dynamic growth, so the documented WCP-15 example produced a
  parent that refused every later `AddMultiInstance` at WARNING.

- **Engine-written context keys are enumerated and frozen (#37).** The defect was
  never that `_hydrated_keys` leaked; it is that `context` had no published
  boundary between caller data and engine bookkeeping, so every engine addition
  was a silent behaviour change. `models/stage/engine_keys.py` publishes all 38;
  `tests/test_engine_context_keys.py` fails when an unregistered key appears or a
  registered one stops being written. 36 of the 38 are now stripped from a task's
  `INPUT`; `_signal_name` and `_signal_data` stay visible because WCP-24 delivers
  a signal to a suspended task through context.

  An `_engine` envelope was designed for this and **withdrawn**: engine keys
  legitimately vary by execution path, so nesting them stabilises no consumer's
  key count, and under a no-migration commitment historical rows keep the old
  shape indefinitely. The enumeration delivers what the envelope was chosen for
  and covers every key rather than three.

- **The dependency manifest did not match the code, in both directions (#36).**
  `PyYAML` was imported by `cli/config.py` and declared nowhere, so `mg-up` and
  `mg-status` exited 1 on an `mg.yaml` host; it is now declared in a new `cli`
  extra. `psycopg-pool` arrived through psycopg's own `[pool]` extra and is now
  declared explicitly. `pydantic` was declared and imported nowhere — it is
  **kept**, because the declaration was intent that was never implemented, and
  issue 41 implements it.

- **`workflow_signals` is created on SQLite (#16, partial).**
  `create_signals_table` existed and was never called, so the table was created
  by migration on PostgreSQL and did not exist at all on SQLite. The storage move
  itself is still open.

- The `pyproject.toml` dependency comment claimed a cap here is the estate-wide
  binding constraint. It is not: bulkman pins `resilient-circuit[postgres]<0.9`,
  deliberately, because it drives a private attribute. Corrected to name the
  location and the reason.

### Added

- **`tests/test_dependency_manifest.py`** compares imports against declarations
  in both directions, with a test proving the scanner can find an import it is
  pointed at, and a self-expiring exemption map. That map caught its own stale
  entry within the same session.

- **24 probes in `audit/evaluations/`**, each carrying controls that fail if the
  probe goes vacuous. New here: task-lease fail-closed, event-store DDL, message
  contract, structured loops, engine-key runtime census across four graph shapes,
  dynamic multi-instance, and multi-tenant RLS.

- **Multi-tenant RLS is pinned by a test (#20).** Two tenants on separate
  per-tenant DSNs against real PostgreSQL, with `tenant_id` defaulted from a GUC
  under RESTRICTIVE policies: both workflows succeed, each tenant sees only its
  own rows, and `FOR UPDATE SKIP LOCKED` is permitted. Note for deployers: a
  purely RESTRICTIVE policy set denies everything, because restrictive policies
  only subtract from permissive ones — a correct deployment needs a permissive
  grant alongside the restrictive tenant constraint.

### Known limitation, unchanged

- Stage context is persisted verbatim and is a bind parameter on every state
  transition, so anything a caller places there is readable by anyone with
  SELECT on `stage_executions`. This is a deliberate scope boundary (#17): the
  orchestrator records what it is handed. Redaction at the persistence boundary
  was implemented and reverted on measurement — handlers re-read stage state
  during execution, so it starves the running task, which receives the literal
  string `***REDACTED***` while the workflow reports SUCCEEDED.

## [0.27.0] - 2026-09-20

Ten orchestration defects, found by a three-pass adversarial audit. Every one was
reproduced through the public API before being fixed, and each carries a probe in
`audit/evaluations/` that has been shown to fail as well as pass.

### Upgrade notes — observable behaviour changes

These are corrections to defects, but they change what existing workflows do.
Read these before upgrading:

- **A de-selected branch no longer runs, and neither does anything behind it.**
  Previously a branch deeper than one stage ran even when its split de-selected
  it, because SKIPPED counted as "upstream satisfied". If a pipeline was relying
  on that, the branch it wants must now be selected by the split's condition.
- **A join whose every branch was de-selected is now SKIPPED rather than
  executed.**
- **`MULTI_MERGE` now fires once per upstream** instead of once in total. A stage
  using it will execute N times where it previously executed once.
- **A re-entered stage now re-reads its ancestors** instead of keeping the first
  value it saw. Jump-based retry loops that silently never converged will now
  converge — and will run to their real exit condition rather than failing at the
  jump budget.
- **Structured loops execute.** `LoopBuilder` workflows previously died on
  `TaskNotFoundError`; they now run, so a graph built with one will do work it
  did not do before.
- `stageEnabled=False` deliberately still does **not** prune: a disabled stage in
  a linear pipeline continues to let its successor run.

Not fixed in this release, and still broken: multi-instance `cancel_remaining`
has no runtime reader, so the remaining instances run to completion; and
`create_dynamic(initial_count > 0)` clears `allow_dynamic`, so later
`AddMultiInstance` messages are refused. Both are now flagged in the docs and the
agent prompt rather than reading as working features.

### Fixed

- **A re-entered stage now observes fresh upstream outputs (#27).** `_plan_stage`
  merged ancestor outputs, let the stage's own context override them, then
  persisted the merged result back onto the stage — so the first value a stage
  ever saw for a key was frozen into its row and shadowed every later upstream
  output.

  This made every jump-based retry loop structurally unable to observe progress.
  Reproduced through the documented `jump_to` pattern (WCP-10), no `LoopBuilder`
  involved:

      upstream produced attempts = 11
      checker observed each pass = [1,1,1,1,1,1,1,1,1,1,1]
      workflow = TERMINAL

  The upstream had incremented to 11 and its persisted outputs said so; the
  downstream saw 1 on all eleven passes, so its exit condition could never become
  true. It exhausted the jump budget and the workflow failed with a misleading
  "Max jump count exceeded". Callers could not work around it — the stage is
  reloaded from the store before a task's result is processed, so in-place
  context mutation is discarded.

  Hydration now records which keys came from ancestors; on a later plan those
  keys yield to the current ancestor value. A key set directly on the stage still
  wins, and a stage planned only once is unaffected — the new path is reached
  only on re-entry. After: `[1, 2, 3]`, three iterations, `SUCCEEDED`.

- **The event read path tolerates events written by a newer build (#28).** Five
  unguarded `EventType(...)` sites raised `ValueError` on an unrecognised string,
  and the exception escaped row-to-event conversion — so a single unknown row
  failed the *entire* query, taking out replay, `WorkflowStream` and every
  durable subscription over that store, not just that event. Separately,
  `EventMigrator.migrate` walked only forward, so an event from a newer schema
  fell through unchanged and was applied with the wrong field layout, silently.

  Unrecognised types now resolve to `EventType.UNKNOWN` with the original string
  preserved in `data["_raw_event_type"]`; a newer schema is refused under strict
  migration and skipped during replay.

  No behaviour change — no code path today can produce either condition. This
  ships ahead of any new event type deliberately: without it, a rolling deploy or
  a mixed-version fleet would have older readers crash on rows newer writers
  produce.

- **PostgreSQL durable subscriptions no longer lose an event whose transaction
  commits out of sequence order (#29).** `events.sequence` is `BIGSERIAL`,
  assigned at INSERT and not at COMMIT, so a reader advancing its cursor to the
  highest visible sequence stepped over a lower sequence still held open by
  another transaction. When that transaction committed, its event sat below the
  cursor and was never delivered — silently and permanently. Because events join
  the workflow state transaction, those transactions are long-lived, so the
  window was wide rather than theoretical.

  Reproduced on PostgreSQL 16; the probe shows both strategies in one run:

      delivered (commit cursor):   ['slow-09a63f', 'fast-90d330']
      delivered (sequence cursor): ['fast-90d330']

  Delivery is now ordered by a `commit_xid` column against a
  `pg_snapshot_xmin()` watermark — the lowest transaction id still in progress,
  and therefore a frontier nothing can later commit beneath. Events written
  before the column exists remain deliverable.

  Requires PostgreSQL 13+ for `xid8`; below that the engine keeps the sequence
  cursor and logs a warning naming the loss mode. SQLite is unaffected, its
  write lock having always made commit order equal sequence order.

  The tradeoff, stated rather than hidden: delivery is held behind the oldest
  in-flight write transaction, so one long workflow transaction delays
  subscription delivery.

- **The event log now records suspension, jumps, retries and who acted (#30).**
  Seven recorder methods shipped with live replay branches and **zero call
  sites**, and there was no event type for suspension at all. The operational
  consequences: a human-approval wait was an unexplained silence between
  `task.started` and `task.completed` and replayed as RUNNING, so an operator
  could not tell "waiting on a person" from "stuck"; a workflow that looped
  forty times replayed as though it ran once; and a retry storm was invisible.

  Separately, every event in the system was attributed to `"system"` — the
  recorder's context function accepted an actor, but the handler wrapper
  dropped it — so "who released the production gate" had no answer on any
  surface.

  Now recorded: `stage.suspended`, `stage.resumed` (carrying the signal name),
  `jump.executed` (with the jump type, including restarts), and `task.retried`.
  Each is written inside the transaction that commits the state it describes,
  so a rollback cannot leave a phantom event. `hitl.approve`/`reject`/
  `send_signal` take an optional `user=`, carried on the message rather than
  merged into `signal_data` — which tasks expose verbatim as outputs — and
  recorded as the event actor. Replay understands suspension, so a waiting
  workflow replays as SUSPENDED rather than RUNNING.

  Actions with no known identity still record as `"system"`; no actor is
  invented.

- **Structured loops (WCP-21) execute for the first time (#31).**
  `LoopBuilder` emitted stages naming `LoopConditionTask` and `LoopBackTask` —
  classes that existed nowhere in the codebase — so every loop built by the
  public API died at its first condition check with `Task type not found` and
  the workflow went TERMINAL. `LoopBuilder` is exported, documented in the
  guide, and emitted verbatim by `stabilize prompt`, the reference the README
  tells you to hand your coding agent. It had no tests.

      while_loop("i < 3")     -> body ran with i = [0, 1, 2]   SUCCEEDED
      while_loop("i < 0")     -> body ran 0 times              SUCCEEDED
      while_loop(max_iter=4)  -> 4 runs, FAILED_CONTINUE       SUCCEEDED
      repeat_until("i >= 2")  -> body ran with i = [0, 1]      SUCCEEDED

  Reaching `max_iterations` now exits the loop and continues past it, carrying
  `loop_exhausted` downstream and recording the failure in the terminating
  stage's status, rather than exhausting the generic jump budget and failing the
  workflow with a misleading "Max jump count exceeded". A condition referencing
  an identifier nothing publishes fails loudly instead of spinning to the bound.

  `TaskRegistry` now seeds the task classes the engine's own builders emit; a
  caller registering the same name still takes precedence. This also fixes
  `WaitStageBuilder`, whose `WaitTask` existed but was never registered.

  Nested loops work too (#32): an inner loop gets a full, independent budget on
  every pass of the outer loop. That needed the deterministic merge above, and
  the loop condition publishing its iteration counter — a loop-back was
  otherwise counting up from its own copy, left over from the previous outer
  pass, and reaching a tight inner bound early.

- **The ancestor-output merge is deterministic across processes (#26).** Both
  backends seeded a topological sort from a `set`, and Python randomises string
  hashing per process — so which ancestor won a key collision in a diamond
  depended on which worker planned the stage. Two replays of the same workflow
  could disagree:

      before:  4 from_b / 4 from_c    across 8 hash seeds
      after:   8 from_c               deterministic

  The algorithm was duplicated verbatim in both backends and is now shared, so
  they cannot drift apart again.

  A stable tie-break alone would not be enough: with a total order imposed, the
  winner becomes whichever `ref_id` sorts last, so renaming two branches would
  silently change which value survives. The merge now also **reports** a
  collision between ancestors with no path between them, naming both ancestors
  and both values, and stating that the tie-break is a convention rather than a
  semantic. `STABILIZE_MERGE_STRICT=1` raises instead of warning.

  The repository's own guard against this ran all ten of its iterations inside
  one interpreter, where hash order is fixed, so it could never fail. It now
  spawns subprocesses.

- **A jump's context survives re-entry hydration.** The re-entry fix above makes
  a stage prefer its ancestor's value over its own stale copy, which is right in
  general — but a jump's carried context is a deliberate write, not a stale copy,
  and was being overwritten by it. A loop-back carrying a fresh counter had it
  reset to the enclosing scope's value. The jump now exempts the keys it writes,
  for the following plan only.

- **A de-selected branch no longer executes, and a join with no live branch no
  longer fires (#33).** An OR-split marked its non-activated children SKIPPED,
  but SKIPPED counts as "upstream satisfied" — so the rest of that branch ran
  anyway. A documented XOR-split gate was therefore not a gate: a disabled
  production deploy still deployed. A join whose every upstream branch had been
  de-selected also fired, executing a merge point on a path no token reached.

      before:  two-deep split   -> ran ['dead2', 'live', 'live2', 'root']
               all-pruned join  -> ran ['live', 'orjoin', 'root']
      after:   two-deep split   -> ran ['live', 'live2', 'root']
               all-pruned join  -> ran ['live', 'root']
               diamond control  -> ran ['join', 'left', 'root']

  The decision is made at each child's own readiness evaluation, the only place
  where all of that child's upstreams are visible — a child with another live
  parent still carries a token. Pruned stages become SKIPPED, so there is no new
  status and no change to persisted shapes, and absence of a marker means live,
  so a workflow upgraded mid-run behaves exactly as before.

  Also removed `_record_activated_branches`, which iterated a partial workflow
  that never contained the downstream join — so it wrote nothing — and wrote
  outside its caller's transaction.

  **Behaviour change:** a join with no live branch is now SKIPPED rather than
  executed, and a de-selected subtree no longer runs. `stageEnabled=False`
  deliberately still does not prune, so a disabled stage in a linear pipeline
  continues to let its successor run.

  MULTI_MERGE (WCP-8) still fires once rather than once per upstream completion;
  that needs a separate re-arm design and is not part of this change.

- **MULTI_MERGE (WCP-8) fires once per upstream completion (#34).** It was
  implemented as a readiness predicate that returned READY whenever *any*
  upstream completed, with the firing bookkeeping delegated to a caller branch
  that was never written — so the 2nd..Nth triggers were swallowed and the stage
  behaved as an AND-join that ignored its other parents.

      before:  mm ran 1x for 3 upstreams
      after:   mm ran 3x, each upstream consumed exactly once

  `start_stage/handler.py` has a post-claim, per-join-type firing hook with
  branches for DISCRIMINATOR and N_OF_M; it has had none for MULTI_MERGE since
  the commit that created all three. The re-arm happens at **completion**, not
  when the next token arrives: the token typically arrives while the stage is
  still RUNNING, and `RestartStage` already refuses to re-arm a non-terminal
  stage for exactly that reason. `StartStage` gained a trailing defaulted
  `triggering_upstream_ref_id`, so a firing can tell which branch triggered it
  via `context["_mm_trigger"]`.

  Three limits, now stated in the guide, the enum docstring and the agent-facing
  prompt rather than implied: firings are **serialised**, not concurrent;
  `stage.outputs` holds the **last** firing with earlier ones archived in
  `context["_mm_firings"]`; and the multiplicity **does not propagate** — the
  merge stage's own downstream runs once, since carrying a thread of control per
  token would need a separate stage row per token.

### Security

- **Stage-level messages are now scoped to the workflow they name (#25).**
  `with_stage` resolved `message.stage_id` by primary key and never compared the
  resolved stage's workflow to `message.execution_id`, and no handler did either.
  Every `StageLevel` message was affected: `SignalStage`, `CancelStage`,
  `SkipStage`, `JumpToStage`, `RestartStage`, `ResumeStage`, `AddMultiInstance`.

  Reproduced through the public API alone — two workflows each suspended on an
  `ApprovalTask`, then
  `hitl.approve(queue, execution_id=B.id, stage_id=<A's stage id>)` released
  **workflow A's** approval gate:

      before: A=SUSPENDED B=SUSPENDED
      after:  A=SUCCEEDED outputs={'approved': True, 'approval': {'by': 'caller-in-B'}}

  The gate was the only thing separating two workflows' human approvals, because
  an approval URL has to carry an internal stage ULID — `hitl` offers no
  correlation-key alternative. The dedup row was also written under the *sending*
  workflow's id, so the trail attributed A's approval to B.

  A mismatched message is now refused: the stage is not mutated, the message is
  marked processed so it is not redelivered, an `InvalidStageId` marker is
  emitted, and a WARNING names both the claimed and the actual workflow. No
  shipped code path passes a deliberately mismatched `execution_id`, so
  correctly addressed messages are unaffected — verified in both directions on
  SQLite and PostgreSQL.

  `hitl.send_signal`, `approve` and `reject` accept an optional `store=`; when
  given, a cross-workflow stage id raises `ValueError` at the call site instead
  of being refused asynchronously. This is additive and opt-in.

  Found by the refutation pass of the 2026-09-19 orchestration audit. Probe:
  `audit/evaluations/probe_stage_message_ownership.py`, registered in
  `run_all.sh` and verified to go red with the guard removed.

## [0.26.0]

### Security

- **`mg-up` and `mg-status` no longer discard the TLS settings from the
  database URL.** `parse_db_url()` kept only host, port, user, password,
  dbname and stabilize's own `schema`. **Every other query parameter was
  dropped**, including `sslmode`, `sslrootcert`, `sslcert` and `sslkey` — so a
  URL asking for `sslmode=verify-full` reached a TLS-mandatory database with
  no TLS settings at all, silently. An operator who asked for a security
  control got no control and no warning; the server reported "connection
  requires a valid client certificate", then "no encryption".

  Reported by trace-thinkpad-83589d against 0.25.2. All query parameters now
  pass through to libpq; the URL's own components still win over a same-named
  query parameter, and `schema` remains stabilize's and is not forwarded.

  Verified live against a server with SSL off, both directions: no `sslmode`
  connects, `sslmode=require` is refused with "server does not support SSL,
  but SSL was required", and `sslmode=disable` connects again — so the guard
  is honouring the parameter rather than failing blanket.

  `connect_timeout`, `application_name` and every other libpq parameter were
  dropped by the same code and are fixed by the same change.

### Changed

- **No upper bound on `bulkman` or `resilient-circuit`.** A cap here is the
  binding constraint estate-wide, because stabilize is pulled in transitively
  almost everywhere, and it blocks their fixes from reaching anyone. Floors
  are now their current releases: `bulkman>=2.0.4`, `resilient-circuit>=0.8.2`.

  The tradeoff, stated rather than hidden: a future breaking major installs
  silently. The engine is built to survive that rather than prevent it — a
  breaker store it cannot construct is reported at ERROR naming the
  consequence, and `STABILIZE_CIRCUIT_STORAGE_STRICT=1` makes it a startup
  failure instead of process-local state.


## [0.25.2]

### Fixed

- **A DSN carrying its own `options` is no longer overridden by `schema=`.**
  0.25.1's precedence guard inspected `PoolOptions.connect_kwargs` only, so it
  could not see an `options=` parameter inside the DSN string — and a psycopg
  keyword argument beats the conninfo, so `schema=` silently replaced the
  caller's own `search_path`:

      DSN "?options=-csearch_path%3Dalpha" + schema="beta"  ->  beta   (wrong)

  Harmless only while both name the same schema, which is exactly the shape of
  the natural upgrade path ("add `schema=`, leave the DSN alone"). It would
  have surfaced first on a second schema, as `UndefinedTable` from a deployment
  whose DSN plainly said otherwise. `with_schema()` now takes the connection
  string and defers when the DSN sets options, in either spelling (URL query
  parameter or keyword/value).

  Reported by vellum-build-d8bbd2, who also verified 0.25.1's three fixes on
  PostgreSQL 18.4 / FreeBSD over mutual TLS — a platform none of this had been
  measured on.

### Notes

- The declared `resilient-circuit>=0.5.0,<3.0.0` range was verified at its
  floor: the full suite collects 1507 and passes identically on 0.5.0 and on
  0.8.2. The ceiling is not verified and cannot be — it admits versions that
  do not exist yet.


## [0.25.1]

### Fixed

- **`exists()` no longer reports a broken deployment as an empty one (#24).**
  It wrapped `retrieve_execution_summary` in `except Exception: return False`,
  so a missing TABLE and a missing ROW gave the same answer. Reported by
  vellum-build-d8bbd2, who wrote a check to prove their schema workaround was
  needed and found it passed in **both** directions — the check could not fail.
  `exists()` now catches only the not-found errors; an operational failure
  (missing relation, unreachable database, permission denied) propagates.

- **`PostgresWorkflowStore` and `PostgresQueue` accept `schema=` (#24).**
  `mg-up` honours `MG_SCHEMA`, but the runtime queried bare table names and
  looked in whatever `search_path` gave it. The schema is applied as a libpq
  `-c search_path=` connect option, reusing the `PoolOptions` seam from #18:
  queries stay as written, and pools are keyed by options, so two schemas get
  two pools rather than sharing one. The name is validated as a plain
  identifier before it reaches the option string.

- **`mg-status --db-url` honours `MG_SCHEMA` (#24).** That path called
  `parse_db_url` directly and skipped `load_config`, where the override lived,
  so it reported `relation "stabilize_migrations" does not exist` — which reads
  as "nothing has ever been applied" rather than "I am looking in the wrong
  schema". Both entry points now share `apply_schema_override()`.

### Notes

- Verified against resilient-circuit 0.8.2 and bulkman 2.0.4; the declared
  bounds already admit it and are unchanged.


## [0.25.0]

### Security

- **HTTPTask refuses SSRF at connect time, on the real peer address (#3).**
  Validating a hostname and letting urllib resolve it again is a TOCTOU: an
  attacker controlling DNS answers the validation lookup with a public address
  and the connect lookup with a private one. Guarded connection classes now
  check `getpeername()` after connect and before any request bytes are written.
  Checking the live peer rather than pinning a pre-resolved IP keeps TLS
  hostname verification and SNI intact, which naive pinning breaks.

  Found while building the probe: HTTPTask revalidates the URL immediately
  before `open()` at two sites, that revalidation raises `ValueError`, and
  neither `except` clause caught it — so a rebinding attempt escaped
  `execute()` as an **unhandled exception** rather than returning a terminal
  result. A blocked request crashed the task. Both sites now catch it, and an
  SSRF refusal is never retried, since a retry is the attacker's next
  resolution.

### Added

- **Caller-supplied PostgreSQL pool options (#18).** `PoolOptions` carries
  libpq connect kwargs, a psycopg `configure` callback, pool sizes and an
  acquisition timeout. Pools are keyed by connection string **and** options, so
  callers asking for different options no longer silently share whichever pool
  was created first. With no options, connections inherit the server defaults
  and carry **no `statement_timeout` and no `lock_timeout`** — now documented
  rather than implied.
- **A synchronous processor warns when nothing renews the queue lease (#21).**
  `process_all()`/`process_one()` start no lock heartbeat, so a handler
  outliving `lock_duration` makes its message visible mid-flight and a later
  poll can execute it again — silent double execution. The first synchronous
  poll now warns once, naming the queue, the duration and both remedies. This
  makes the condition detectable; it does not prevent it, which needs either a
  renewal thread or an operator-sized lease.

### Fixed

- **A pool is no longer closed out from under other holders (#19).** Pools are
  holder-counted: a store and a queue built on one DSN no longer close each
  other's pool, and the pool still closes when the last holder releases it.
- **`is_healthy()` answers within a bounded interval (#22).** It borrowed with
  psycopg_pool's 30s default, so the failure path — the only path it exists for
  — read as a probe timeout rather than a negative answer. Default bound is now
  2.0s, configurable per store. Measured: 1.000s against an unreachable
  database, where it previously took ~30s.
- **An unreachable N_OF_M join is refused at submit time (#4).** A join whose
  threshold exceeds its upstream count can never become ready. The engine
  already detected this in `recovery.py`, during a crash-recovery sweep, long
  after the workflow had run and stalled.

### Tests

- Two DLQ tests stopped skipping SQLite on an unmeasured claim (#13). The
  stated reason — "SQLite doesn't handle high-concurrency DLQ operations
  reliably" — was never true: the fixture was `:memory:`, which is
  per-connection, so ten threads addressed ten databases. Both now run on
  SQLite and pass. Half the backend matrix had silently never run.


## [0.24.0]

### Changed

- **`resilient-circuit` is now `>=0.5.0,<3.0.0` and `bulkman` is `>=2.0.4,<3.0.0`.**
  The previous `resilient-circuit<0.8` cap was the binding constraint estate-wide,
  because stabilize is pulled in transitively nearly everywhere. It was held while
  `_create_storage()` silently swallowed `SchemaNotReady`; 0.23.0 and 0.23.1 removed
  that, so the cap is no longer a safety hold.

  **The `bulkman` floor moves to 2.0.4 for a reason that is easy to miss:** every
  earlier 2.x caps `resilient-circuit` below 0.8 — 2.0.1 at `<0.5`, 2.0.2 at `<0.6`,
  2.0.3 at `<0.8`. Raising only the `resilient-circuit` cap would let a resolver pick
  a bulkman that forbids 0.8.x, producing an environment that satisfies neither
  intent. Verified in a clean venv: `pip install stabilize` now resolves
  bulkman 2.0.4 with resilient-circuit 0.8.1, and `pip check` is clean.

  Under resilient-circuit 0.8.x, `PostgresStorage` no longer issues DDL at
  construction and raises `SchemaNotReady` on a database that has not been through
  `resilient-circuit-cli pg-setup`. stabilize reports that as an ERROR naming the
  consequence, and `STABILIZE_CIRCUIT_STORAGE_STRICT=1` turns it into a startup
  failure rather than process-local breaker state. Deployments that relied on the
  old auto-create behaviour can set `RC_DB_AUTO_CREATE=1`, or run `pg-setup`.

  Verified against resilient-circuit 0.8.1 and bulkman 2.0.4: full suite
  1431 passed, 7 skipped, 1 xfailed.


## [0.23.1]

### Fixed

- **`postgres://` and libpq keyword/value DSNs no longer select process-local
  circuit-breaker state.** `_create_storage()` gated on
  `database_url.startswith("postgresql")`, which misses three forms:
  `postgres://` (short form), an upper-case scheme, and a libpq keyword/value
  string. Each miss fell through to `InMemoryStorage()` and logged
  `Using in-memory storage for circuit breakers (SQLite or no database)`.

  `postgres://` is not an exotic spelling: stabilize's own `parse_db_url()`
  accepts it (`postgres(?:ql)?://`) and its own `build_db_url()` **emits** it.
  So the engine produced a DSN that its own breaker selector then classified
  as "no database", and said so in the log. Anyone using that form has had
  process-local breakers on every version, with a log line asserting they had
  no database configured.

  The scheme is now parsed rather than prefix-matched (`postgres`/`postgresql`,
  optional `+driver`, case-insensitive, whitespace-tolerant) and a libpq
  keyword/value DSN is detected by keyword (`host`, `hostaddr`, `dbname`,
  `service`).

- **The in-memory log line no longer claims SQLite for every case.** It said
  "SQLite or no database" whatever the reason, including for PostgreSQL DSNs
  it had just misclassified. It now states only what is true: no PostgreSQL
  DSN was configured, and circuit state is process-local.

### Notes

- `probe_circuit_storage_honesty.py` now asserts DSN classification in both
  directions — `sqlite`, `None` and `""` must **not** reach the PostgreSQL
  branch, so the probe cannot pass by routing everything there.
- The `resilient-circuit>=0.4.6,<0.8` bound is again deliberately unchanged.


## [0.23.0]

### Fixed

- **Circuit-breaker storage no longer degrades silently.** A `postgresql://`
  URL is an explicit request for breaker state SHARED across instances.
  When `PostgresStorage` could not be constructed, `_create_storage()` caught
  the exception, logged a single WARNING, and returned `InMemoryStorage()` —
  so circuit state silently became process-local and a breaker open on one
  worker stayed closed on every other one. Reported by infra-manager-c13110
  in the context of resilient-circuit 0.8.x, where a missing or drifted
  breaker table raises `SchemaNotReady` on a database that has not been
  through `pg-setup`.

  The failure is now logged at ERROR and names the consequence explicitly.

- **The log no longer claims a backend it does not have.** `Using PostgreSQL
  storage for circuit breakers` was emitted at INFO *before* construction was
  attempted, so it appeared even when construction then failed. An operator
  grepping for that line got a false confirmation. It is now emitted only
  after the storage object exists.

### Added

- `STABILIZE_CIRCUIT_STORAGE_STRICT=1` makes an unusable PostgreSQL breaker
  store abort startup with `CircuitStorageUnavailableError` instead of
  degrading. Unset (the default), behaviour is unchanged apart from the log
  level, so this release is safe to take without a configuration change.
- `audit/evaluations/probe_circuit_storage_honesty.py`, which tests both
  directions: that degradation is loud and names its consequence, and that
  strict mode fails closed.

### Unchanged

- The `resilient-circuit>=0.4.6,<0.8` bound is **deliberately not moved** in
  this release. Widening it is an estate-wide decision, and on any database
  not put through `pg-setup` it would produce process-local breakers unless
  `STABILIZE_CIRCUIT_STORAGE_STRICT` is set.


## [0.22.1]

### Security

- **`mg-up` / `mg-status` no longer print the database password.** On a URL
  that failed to parse, `parse_db_url()` echoed the whole DSN — password
  included — to stdout, on both the `--db-url` and `MG_DATABASE_URL` paths.
  Reported by provenance-50ca06 and independently reproduced by
  infra-manager-c13110 against 0.22.0 from PyPI in a clean venv; one database
  credential was rotated as a result. Errors now print the DSN with the
  password replaced by `***`, covering `postgres://` userinfo, a host-less
  `user:password`, and libpq keyword/value secrets (`password=`, `passfile=`,
  `sslpassword=`). Control characters are escaped so a crafted DSN cannot
  forge a second log line, and the echo is length-capped.

- **The mg CLI no longer corrupts its own connection parameters.** The libpq
  conninfo was built by string interpolation, and a URL carrying no password
  substituted `""`, producing the text `password= dbname=<db>`. libpq skips
  whitespace after `=`, so the password swallowed the next parameter. One
  defect, four symptoms: `dbname` vanished entirely and libpq defaulted the
  database to the *username*; the forged password overrode `PGPASSWORD`; a
  password containing a space raised `ProgrammingError`; and a password
  containing `dbname=evil` redirected the connection. Connection parameters
  are now passed as a mapping to `psycopg.connect(**params)`, so no conninfo
  string is built and an absent password is omitted rather than sent as `""`.

  The defect had two independent maskers, either sufficient to hide it: a
  permissive `pg_hba.conf` fails authentication before the database is
  checked, and an exported `PGDATABASE` silently supplies the missing name.

- **Userinfo in a database URL is now percent-decoded.** A password
  containing `@` or `/` must be percent-encoded in a URL, and was previously
  sent to the server literally (`pw%20with%20space`).

- **HTTPTask SSRF guard rewritten around address classification.** The
  blocklist was a hand-maintained CIDR list and missed whole classes:
  IPv4-mapped IPv6 (`::ffff:127.0.0.1`, `::ffff:169.254.169.254`) matched no
  IPv4 CIDR and is not reported as loopback by `IPv6Address.is_loopback`, so
  loopback and cloud-metadata targets passed outright; `0.0.0.0` and `::`
  were unblocked; so were `100.64.0.0/10`, multicast and reserved ranges.
  A DNS resolution failure returned early and **allowed** the URL. There was
  no scheme allowlist, so `ftp://` and `gopher://` reached urllib. Addresses
  are now normalised before checking, classified with the stdlib's own
  predicates, DNS failure fails closed, and only `http`/`https` are permitted.

- **HTTPTask no longer persists credentials from a request URL.** The URL,
  including any userinfo password, was written into stage `outputs` and the
  debug log. Outputs propagate to downstream stages and the monitor, so the
  engine amplified an author-supplied secret beyond the stage that declared
  it. Outputs and logs now carry the redacted URL. A URL in the stage's own
  `context` is unchanged, because that is the stage definition needed to
  execute or replay it — credentials belong in headers or `secrets`.

### Added

- `stabilize.redaction` with `redact_userinfo()` and `redact_db_url()`,
  shared by the CLI and the HTTP task.
- `audit/evaluations/` probe harness with `run_all.sh`. `probe_mg_conninfo.py`
  verifies connection parameters against a PostgreSQL wire-protocol listener,
  reporting the startup packet and password message, so neither the `pg_hba`
  nor the `PGDATABASE` masker can hide a regression. `probe_ssrf_guard.py`
  tests the guard in both directions. `probe_http_credential_persistence.py`
  drives a real workflow and reads state back through `WorkflowStore`.


## [0.22.0]

### Fixed
- **Persistent signals (WCP-24) no longer accumulate without bound in
  `stage_executions.context`.** `SignalStageHandler` buffered a persistent signal
  whenever the target stage was not `SUSPENDED`, and the only drain
  (`handlers/run_task/result.py`) pops one entry on a suspend *edge*. A stage in a
  complete status can never reach another suspend edge, so every signal sent to it
  was appended to a JSON list that nothing could ever read. Reported by
  ci.rodmena.co.uk against 0.21.1 on PostgreSQL: 1,259,015 buffered entries across
  105 stage rows, of which 1,258,972 (99.997%) sat on 39 CANCELED or TERMINAL rows,
  the largest holding 112,901 copies of the same 46-byte object. The row was 93 kB
  on disk and 5,405 kB uncompressed — a 58x TOAST ratio, because the entries are
  identical — and PostgreSQL sent the uncompressed value as a bind parameter on
  every state transition, logging it each time.

  Measured by infra-manager on the affected host (pg-nano-03, shared by six
  databases): ~5.1 GiB of free space consumed in about 36 hours to 2026-09-06,
  peaking at 1.66 GiB per 6-hour window (~6.6 GiB/day at the filesystem, against
  a ~0.25 GiB/day baseline), and 3.76 GiB reclaimed on remediation. Free space
  bottomed at 14.46 GiB of 38 GB. No other database on the host was degraded.
  A separate outage on that host on 2026-09-12 was an unrelated memory
  misconfiguration and is not attributable to this defect.

  Three bounds, none of which changes behaviour for a stage that is genuinely
  waiting:
  - A persistent signal for a stage whose status `is_complete` is **refused**, not
    buffered. It is marked processed so it is neither redelivered nor retried.
  - The buffer holds at most `STABILIZE_SIGNAL_BUFFER_MAX` entries per stage
    (default 1000). Overflow is refused and moved to the dead-letter queue with
    reason `signal_buffer_full`, so it stays inspectable and replayable rather than
    silently dropped.
  - A stage's buffer is cleared when the stage reaches a complete status.

- **`SignalStageHandler` ignored an injected `HandlerConfig`.** It was constructed
  without one in `queue/processor/mixins.py`, so programmatic configuration was
  silently discarded and only the environment variable took effect.

### Added
- **`stabilize prune-signals`** reclaims buffers already stranded in a database by
  an earlier version — the new bounds stop the growth but cannot reach rows whose
  stage has already completed. `--dry-run` reports the count without writing,
  `--status` narrows to named statuses for a staged rollout, and `--include-active`
  extends to stages that could still consume their buffer. An unrecognised status
  name is rejected rather than matching no rows, so a typo cannot report a
  successful cleanup of zero. Backed by `cleanup_buffered_signals()` and
  `count_buffered_signal_stages()` on `WorkflowStore`.
- **`STABILIZE_SIGNAL_BUFFER_MAX`** (default 1000) caps buffered persistent signals
  per stage. The cap is on **array length**, not serialised size: identical entries
  TOAST-compress ~58x, so a byte-based cap sized from `pg_column_size` would admit
  far more than intended, while the real cost — bind-parameter size, parse time and
  WAL per transition — tracks the uncompressed form.

### Changed
- **Refusal logging is rate-bounded.** A log line per refused signal would fire at
  the signal rate and reproduce, at the logging layer, the unbounded growth the
  refusal prevents. Refusals are counted per stage and emitted at WARNING on counts
  1, 10, 100, 1000, ... and on every multiple of 100,000, carrying the running
  total. Decade spacing alone leaves a live producer effectively silent as the gap
  grows geometrically (104 days, then 1,040, at 6 signals/min); the floor bounds it
  so an ongoing problem never looks like a stopped one. Measured: 112,901 refusals
  cost 6 lines, 10,000,000 cost 105. The tracker is LRU-bounded to 1024 stages.

### Fixed (PostgreSQL)
- **`count_buffered_signal_stages` raised `KeyError: 0` on PostgreSQL.** It indexed
  a row positionally against psycopg's `dict_row` factory, so
  `prune-signals --dry-run` — the read-only command — would have crashed on the
  backend it is most needed on. It passed mypy and ruff; no test exercised the
  counting path under the postgres fixture until one was added.

### Known
- Dead-lettering on cap overflow is not itself bounded: a producer hammering a
  stage that is live but never suspends writes a DLQ row per overflow. This is
  row-per-message in a dedicated table with an existing `clear_dlq()`, is the
  storage shape this area is moving toward, and is not reachable by the reported
  incident, whose stages were complete and therefore refused without storing.
- `workflow_signals` — migrated in `01KGHWCP1M2QSDE3TN4GUBLV8A`, indexed, with a
  complete SQLite implementation at `persistence/sqlite/signals.py` — is still
  unwired; confirmed empty in production. Moving buffering onto it is tracked
  separately and deliberately not shipped alongside an incident remediation,
  because it touches the hot path of every suspended stage.

## [0.21.0]

### Changed (dependencies)
- **Adopted `bulkman>=2.0.1,<3`** (was `>=0.1.0`), so the 2.0.x major is taken
  deliberately rather than picked up incidentally by a lock refresh. The direct
  `resilient-circuit` requirement moves to `>=0.4.6,<0.5` to match the floor
  bulkman itself requires, so a resolver cannot select a version bulkman
  rejects. Verified against the wheels PyPI actually serves.
- **Shutdown races are now classified on exception type.** bulkman 2.0.1 adds
  `BulkheadShutdownError`, raised on post-shutdown execute by both the
  deterministic and the submit-race paths (confirmed against the published
  wheel). The interim workaround — a bare `except RuntimeError` gated on our own
  terminal-state flag — is removed. The pre-dispatch `is_shutdown` check is
  deliberately **kept**: it refuses before the call enters the circuit breaker,
  and a shutdown raised from inside a protected call is recorded by
  `resilient-circuit` as a failure (measured: the circuit opens after 2), which
  would trip circuits for workflows that never failed during a rolling restart.

### Fixed (test integrity)
- **Two tests that could not report a failure now can.** Neither was flaky; both were
  green regardless of the state of the code they covered.
  - `test_race_between_check_and_mark_demonstrates_bug` recorded the known message-
    deduplication race with an imperative `pytest.xfail()` inside an `if`, so it
    xfailed when the race reproduced and *passed silently* when it did not — and would
    have stayed green if the race were fixed, or if it got worse. It now asserts the
    desired contract (deduplication admits exactly one worker) under
    `@pytest.mark.xfail(strict=True)`, so closing the race makes it XPASS and fail the
    suite, forcing the marker to be retired deliberately. **The underlying race is
    unchanged and still open** — handlers must remain idempotent.
  - The SQLite thread-local connection test called `pytest.skip("Too many SQLite lock
    failures ... test inconclusive")` at runtime when too few threads obtained a
    connection — disarming itself under exactly the condition it exists to detect. A
    regression in thread-local connection creation reported *skipped*, not *failed*.
    It now asserts, and reports the observed lock errors.

### Fixed
- **Graceful shutdown now bounds bulkhead shutdown.** `LifecycleManager` called
  `TaskBulkheadManager.shutdown(wait=True)` without passing any timeout, so a
  single stuck task held shutdown open indefinitely despite the advertised
  `shutdown_timeout` (measured: still blocked after 6s with a 30s task). The
  remaining budget is now passed down, and an exhausted budget shuts the manager
  down without waiting. This was latent under bulkman 1.x, which ignored the
  timeout argument entirely; bulkman 2.0.0 honors it.
- **A shutdown race no longer permanently fails a task.** bulkman 2.0.0 makes
  shutdown terminal and surfaces a later execute as a bare `RuntimeError` from
  `concurrent.futures`, which callers cannot distinguish from a task's own
  error. `TaskBulkheadManager` now exposes `is_shutdown`, and
  `execute_with_resilience` refuses dispatch to a terminal manager with a
  retryable `TransientError` instead of burning the task's attempts.

## [0.20.0]

Peer-review remediation release. An external 10-finding evaluation of the
engine (by the RunFlow team) was verified finding-by-finding — all 10
confirmed — and fixed, alongside a partner-reported migration defect and a
re-verification of the v0.19.0 audit certificate on this tree (see the
addendum in `AUDIT.md`).

### Fixed (correctness)
- **`Orchestrator.cancel()` / `.restart()` / `.unpause()` now work.** The
  `CancelWorkflow`, `RestartStage`, `ResumeStage` and `PauseTask` messages had
  no registered handler and were consumed and discarded: cancel returned
  success while the workflow kept running, and pausing a workflow *lost* its
  in-flight task. Four new handlers (`stabilize.handlers.workflow_control`)
  implement the documented contracts; pausing parks the task as `PAUSED` and
  resume re-arms and re-dispatches it. The engine's own start-time-expiry
  cancellation path was silently broken by the same gap and now works.
- **A message with no registered handler is no longer silently acked** — it
  raises, retries, and escalates to the DLQ, so a lost instruction is loud.
  The `Invalid*` diagnostic markers get an explicit consuming handler.
- **Poison messages now reach the DLQ in daemon mode.** The poll loop sweeps
  attempts-exhausted messages every `dlq_check_interval_seconds` (default
  30s); previously only `process_all()` swept, so a service using `start()`
  stalled poisoned work invisibly and `size()`-based drain checks never hit
  zero.
- **Invalid stage graphs are rejected at `Workflow.create()`** with named
  errors — `duplicate_ref`, `self_edge`, `unknown_ref` (naming the typo'd
  stage and the missing ref), and cycles (naming the members) — instead of
  surfacing at runtime as "Exceeded max retries waiting for upstream stages".
- **Recovery's duplicate-guard no longer full-scans the queue.** The
  leading-wildcard `LIKE` over `payload::text` became an exact, indexed
  top-level `task_id` lookup on both backends (additive migration
  `add_queue_task_id_index`), which also removes a nested-substring
  false-positive.

### Added
- **`stabilize mg-up`/`mg-status` target schema support** (reported by
  SponsorSignal): `?schema=` in the db URL, `MG_SCHEMA`, or a `schema:` key in
  `mg.yaml`. `mg-up` validates the identifier, creates the schema if missing,
  and applies everything into it; docs cover pointing the runtime at the same
  schema (`options=-csearch_path`) and moving an existing `public` install.
- **Opt-in retention sweep** (`retention_sweep_interval_seconds`, default off)
  cleans `processed_messages` by age and `stage_claims` for **terminal
  executions only** — a live execution's claim is never deleted, since that
  would resurrect the mutex/deferred-choice race it prevents.
- **Startup warning when crash recovery is disabled** on a store-backed
  processor (`from_handler_config()` does not carry recovery settings, so
  env-configured embedders were getting recovery silently off).
- Soft-timeout visibility: thread-mode timeout logs now state the worker
  thread keeps running, and the resilience guide gained a "Timeout Semantics
  by Isolation Mode" section.

### Changed
- `resilient-circuit>=0.4` is declared as a direct dependency (previously
  imported but only present transitively via `bulkman`).
- CI runs the full non-postgres suite (`-k "not postgres"`) instead of
  `-k "sqlite"`, which had been deselecting ~half the tests — including the
  audit certification suite.
- Docs corrected: there is no In-Memory store backend (use
  `sqlite:///:memory:`, per-connection); SQLite journal mode defaults to
  DELETE with WAL opt-in.

## [0.19.1]

Documentation and developer-experience release. No engine behavior changes.

### Added
- **`stabilize prompt` now covers the agentic toolkit.** The built-in reference
  consumed by AI coding agents gained a full "Agentic Workflows" section:
  `LLMClient`, one-shot `LLMTask`, tool-calling `AgentLoopTask`, `@tool`/
  `ToolRegistry`, durable human-in-the-loop approvals (`ApprovalTask` +
  `approve`/`reject`), live streaming (`WorkflowStream` + `emit_progress`),
  fan-in reducers (`output_reducers`), agentic control-flow (`jump_to` loops,
  N-of-M quorum, discriminator race), and a complete runnable template.
  Validated against glm-5.2: given only `stabilize prompt`, a model generated
  and ran a complete multi-agent workflow on the first attempt.
- **New example `examples/research_analyst/`** — a complex multi-agent workflow
  (parallel ReAct researchers, N-of-M join with reducer, discriminator race,
  refine loop, human approval, report sub-workflow, and crash recovery),
  verified end to end on glm-5.2.
- **Docs:** an "Agentic Workflows" guide and API reference page.

### Changed
- **README rewritten** around building simple, mid, and complex agentic
  workflows, and it now leads with pointing a coding agent at `stabilize
  prompt`. Removed the tool-comparison table.

## [0.19.0]

Correctness audit (130-agent adversarial review) + agentic ergonomics. All
changes are additive or crash-bug fixes; the public API and existing tests are
unchanged. See `AUDIT.md` for the full audit record.

### Fixed (crash-consistency & concurrency)
- Durable message dedup is no longer bypassed after a process restart or bloom
  rotation — the negative-cache fast path is opt-in
  (`dedup_trust_negative_cache`) and only trusted when the bloom is hydrated
  from the durable store.
- `CompleteStage` after-stages/on-failure branches mark the source message
  processed in-transaction; a redelivered/stale `CompleteStage` while work is
  in flight is now an idempotent no-op instead of cancelling a healthy
  workflow, and on-failure stages are planned exactly once.
- Zombie `RUNNING` stages (claimed then crashed before planning) re-claim with
  `expected_phase=RUNNING`, so recovery re-plans them instead of wedging.
- Mutex (`mutex_key`, WCP-17/39/40) and deferred choice (`deferred_choice_group`,
  WCP-16) are enforced with an atomic claim row (new `stage_claims` table),
  fixing a TOCTOU race under concurrent workers.
- `QueueProcessor` heartbeats the queue message lock while a handler runs
  (`enable_lock_heartbeat`, default on; `Queue.extend_lock`), so a task
  outliving `lock_duration` is not redelivered and executed twice.
- Event recording joins the enclosing store transaction and defers bus
  publication until commit — no phantom events, no rolled-back state observed
  by subscribers.
- Jumps apply all stage mutations + follow-on messages in one transaction
  (atomic), and `reset_stage_for_retry` re-arms discriminator/N-of-M joins in
  retry loops.
- Finalizers: per-call timeout is actually enforced (a hung finalizer no
  longer blocks shutdown), and finalizers run on stage terminal states
  (completion/cancellation), not only at process shutdown.
- `WorkflowCircuitFactory` locks its circuit cache; metrics projection dedups
  on logical transition; replay drops spurious cross-workflow gap warnings;
  `EventBus` ASYNC delivery preserves per-subscriber order.

### Fixed (tasks & misc)
- `SSHTask` passes the remote command verbatim (over-quoting broke every
  command with arguments).
- Engine exception hierarchy roots on public `stabilize.StabilizeError` /
  `VerificationError` (`except stabilize.StabilizeError` now catches engine
  errors).
- `stage.outputs` / workflow context / `task_exception_details` / signal data
  serialize with `default=str` (non-primitive values no longer crash
  persistence).
- `ProcessIsolatedTaskExecutor` drains the result before joining (large
  results no longer misreported as timeouts); `RunTaskHandler` lease acquire
  no longer leaks state on failure; `DockerTask` names run containers and
  kills them on timeout; `ShellTask` loads libc in the parent (fork-safety);
  `TransientVerificationError` keeps `context_update`; `SqliteQueue.push`
  honors the `connection=` param; `HighwayTask` stops logging the API key;
  malformed `Content-Length` is tolerated; recovery guards duplicate
  `StartTask`.
- `FileAuditLogger` writes its file (global default is log-only);
  `TaskRegistry.register(strict=True)` opt-in; `HTTPTask` `retry_non_idempotent`
  opt-out; `STABILIZE_STRICT_MIGRATIONS` opt-in; monitor shows Postgres queue
  stats.

### Added (agentic ergonomics — additive/opt-in)
- **Streaming**: `WorkflowStream` (replay and/or follow live) + `emit_progress()`
  and a new `EventType.CUSTOM` for task-emitted progress/token events.
- **Human-in-the-loop**: `ApprovalTask` + `approve()` / `reject()` /
  `send_signal()` / `get_signal()` over the durable suspend/signal machinery.
- **Declarative fan-in reducers**: `StageExecution.output_reducers` +
  `stabilize.reducers` (`collect`/`sum`/`merge`/custom) so parallel branches
  stop clobbering scalar keys at a join.
- **LLM toolkit** (`stabilize.llm`, stdlib-only, not imported by the core):
  `LLMClient` (OpenAI-compatible/Ollama), `@tool` + `ToolRegistry`, `LLMTask`,
  and `AgentLoopTask` (bounded ReAct loop as one durable task).
- New example `examples/agent_team/` — a multi-agent software team that
  exercises the full engine end-to-end (verified on ollama.com cloud glm-5.2).

## [0.18.0]

### Added
- **Workflow Control-Flow Patterns (20 of 43)** — based on van der Aalst et al.
- Join types: `JoinType.OR` (WCP-7), `MULTI_MERGE` (WCP-8), `DISCRIMINATOR` (WCP-9), `N_OF_M` (WCP-30)
- Split types: `SplitType.OR` with per-downstream condition expressions (WCP-6)
- Safe expression evaluator (`stabilize.expressions`) for condition evaluation
- Deferred choice pattern: `deferred_choice_group` on `StageExecution` (WCP-16)
- Milestone gating: `milestone_ref_id` / `milestone_status` on `StageExecution` (WCP-18)
- Mutual exclusion / critical sections: `mutex_key` on `StageExecution` (WCP-17, 39, 40)
- Cancel region pattern: `cancel_region` on `StageExecution` (WCP-25)
- Signal-based suspend/resume: `TaskResult.suspend()` and `SignalStage` message (WCP-23, 24)
- Persistent trigger buffering for stages not yet suspended (WCP-24)
- Multi-instance builders: `MultiInstanceBuilder` for WCP-12 through WCP-15
- `MultiInstanceConfig` dataclass for multi-instance stage configuration
- Structured loop builders: `LoopBuilder.while_loop()` and `LoopBuilder.repeat_until()` (WCP-21)
- Sub-workflow task: `SubWorkflowTask` for recursive workflow patterns (WCP-22)
- New handlers: `SignalStageHandler`, `CancelRegionHandler`, `AddMultiInstanceHandler`
- New messages: `SignalStage`, `CancelRegion`, `AddMultiInstance`
- PostgreSQL migration for new stage columns
- Comprehensive test suite for all WCP patterns (62 new tests)

#### Production hardening (all opt-in; defaults preserve existing behavior)
- **Automatic crash recovery**: `QueueProcessorConfig.recover_on_start` and
  `recovery_interval_seconds` run `WorkflowRecovery` from the processor;
  `QueueProcessor.run_recovery()` for manual sweeps
- **PostgreSQL `get_all_pending_workflows`** parity with SQLite (cross-application
  recovery now works on both backends)
- **Cooperative task cancellation**: `is_cancellation_requested()` /
  `raise_if_cancellation_requested()` / `CancellationToken`; `CancelStage` signals
  running tasks (process-isolation mode remains the hard-kill path)
- **Distributed task lease** (`STABILIZE_TASK_LEASE=1`): cross-process
  single-execution guard for a task; window-narrowing (still requires idempotent
  handlers)
- **SQLite WAL opt-in** via `STABILIZE_SQLITE_JOURNAL_MODE=WAL` /
  `SqliteConfig(journal_mode=...)` (default remains `DELETE`)
- **SQLite schema migrations**: version-stamped, forward-only runner; existing
  databases upgrade in place without re-running baseline DDL
- **Event-sourcing upcasting**: global `EventMigrator` applied (leniently) on
  replay so historical events can be migrated to the current schema version
- 74 new tests covering the above on both SQLite and PostgreSQL

### Changed
- `StageExecution` now has 10 new fields for advanced control-flow patterns
- `evaluate_readiness()` dispatches on `join_type` for OR, MULTI_MERGE, DISCRIMINATOR, N_OF_M joins
- `CompleteStageHandler` supports conditional OR-split logic
- `StartStageHandler` supports milestone gating, mutex blocking, and deferred choice
- `QueueProcessor` auto-registers the 3 new handlers (15 total)

## [0.17.0]

### Added
- Event sourcing system (`stabilize.events`) with full audit trail
- Event store backends: SQLite, PostgreSQL, in-memory
- Event bus for in-process pub/sub with sync/async subscriptions
- Event recorder with automatic handler integration
- Projections: `StageMetricsProjection`, `WorkflowTimelineProjection`
- Event replay for state reconstruction and time-travel queries
- Snapshot support for faster replay of long-running workflows
- Durable subscriptions with at-least-once delivery
- Bloom filter deduplication (`stabilize.queue.dedup`)
- Structured error codes (`stabilize.error_codes`)
- Finalizer registry for resource cleanup
- DAG readiness evaluation (`stabilize.dag.readiness`)
- Event sourcing example (`examples/event-sourcing-example.py`)

### Changed
- All examples updated to use event sourcing by default
- Handler integration tests now verify event recording

## [0.16.2]

### Changed
- `QueueProcessor` now auto-registers all 12 default handlers when `store` and `task_registry` are provided
- `SynchronousQueueProcessor` accepts the same parameters as `QueueProcessor`
- `register_handler()` now raises `ValueError` on duplicate registration
- Added `replace_handler()` for explicit handler overrides
- Added `bulkhead_manager` and `circuit_factory` optional parameters to `QueueProcessor`

### Deprecated
- `register_all_handlers()` standalone function (no longer needed)
