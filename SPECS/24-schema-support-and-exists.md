# Spec: exists() hides UndefinedTable; runtime has no schema argument (#24)

Reported by vellum-build-d8bbd2 (Vellum, SAR redaction platform) on 0.25.0,
PostgreSQL 18.4/FreeBSD. Reproduced locally on PostgreSQL 16 with the engine's
10 migrations applied into a schema named `orchestration`.

## EARS requirements

- If a store operation fails for an operational reason, exists() shall
  propagate that error rather than returning False.
- exists() shall return False only when the workflow is genuinely absent.
- The PostgreSQL store and queue shall accept a `schema` argument and find
  their tables there without the caller hand-writing a search_path.
- mg-status shall honour MG_SCHEMA on the --db-url path as it does on the
  MG_DATABASE_URL path.
- A schema name shall be validated before reaching a connection option string.

## Reproduction, before the fix

    exists() on an absent id
      no search_path (tables invisible) -> False
      with search_path (tables visible) -> False

The SAME answer whether the deployment is broken or fine. Vellum wrote a check
to prove their search_path workaround was needed and it passed in BOTH
directions; the check could not fail. One layer down the distinction exists:

      no search_path  -> UndefinedTable: relation "pipeline_executions" ...
      with search_path -> WorkflowNotFoundError: Execution not found: <id>

    mg-status
      MG_SCHEMA + MG_DATABASE_URL -> 10 applied
      MG_SCHEMA + --db-url        -> relation "stabilize_migrations" does not exist
      --db-url alone              -> same

--db-url calls parse_db_url directly and skipped load_config, where the
MG_SCHEMA override lived.

## Design

exists() catches only WORKFLOW_ABSENT_ERRORS (both WorkflowNotFoundError
spellings in the tree; different backends raise different ones).

schema= is delivered as a libpq `-c search_path=` connect option, reusing the
PoolOptions seam added for #18. The queries stay as written, and pools are
keyed by options, so two schemas get two pools rather than silently sharing
one. A caller who already set their own `options` keeps it. The schema name is
validated as a plain identifier first, since it is interpolated into a string.

## Verified live (audit/evaluations/probe_schema_and_exists.py)

    control: pipeline_executions lives in 'orchestration'
    exists(), no schema configured -> raises UndefinedTable
    exists(), schema='orchestration' -> False
    retrieve_execution_summary, schema= -> WorkflowNotFoundError
    mg-status --db-url, MG_SCHEMA set   -> applied rows
    mg-status --db-url, MG_SCHEMA unset -> relation does not exist

## The probe's own first draft was wrong

Its control reported "applied rows: True" with MG_SCHEMA unset. The check was
`"applied" in text`, which matched `applied_at` in the SQL echoed inside the
error message. The probe had the same defect class it was testing for: a
substring that could not distinguish the two cases. It now matches the row
prefix and prints the first line of output so the result is auditable.

## Follow-up: DSN options precedence (found by vellum-build-d8bbd2 in the fix)

The first version of this fix claimed a caller's own `options` were honoured.
That was true for ONE of the two ways a caller sets them:

    PoolOptions(connect_kwargs={"options": ...}) + schema="beta" -> alpha  correct
    DSN "?options=-csearch_path%3Dalpha"         + schema="beta" -> beta   WRONG

with_schema() inspected PoolOptions.connect_kwargs only. It could not see an
`options=` parameter inside the DSN string, and a psycopg keyword argument
beats the conninfo, so the DSN's search_path was silently replaced.

Harmless only while both name the same schema, which is exactly the shape of
the natural upgrade path ("add schema=, leave the DSN alone") — so it would
have surfaced first on a second schema, as UndefinedTable from a deployment
whose DSN plainly said otherwise.

with_schema() now takes the connection string and defers when the DSN carries
its own options, in either spelling (URL query parameter, keyword/value).

Verified live against two real schemas, both directions:

    DSN(alpha) + schema=beta        -> alpha   the DSN wins
    DSN(alpha), no schema=          -> alpha
    plain DSN  + schema=beta        -> beta    schema= is not dead
    plain DSN  + schema=alpha       -> alpha
    PoolOptions(alpha) + schema=beta-> alpha   unchanged

I asserted this precedence to Vellum without measuring the DSN half. They
tested it because I said it, which is the only reason it was found.
