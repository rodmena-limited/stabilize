# #94 Purge queries use indexes (reported by ci-conductor, thread 01M4SV63YZPAH8TVZK1D4NBBQW)

Status: SUSPECTED cost defect. No index on pipeline_executions includes end_time, and no queue table has an execution_id index (confirmed from migrations/). Existing pipeline_executions indexes: application, pipeline_config_id, status, created_at DESC, (application, status), (pipeline_config_id, status).

Measured by ci-conductor on pg-nano-03 (2026-10-13 05:22Z, 2781 rows): the application-filtered pass used idx_execution_application_status, 0.236 ms execution, 11 shared buffers. Trivial at that size; the cost at 1 M rows is unmeasured.

## EARS SPEC
- When `completed_workflow_ids` runs on PostgreSQL, the store shall resolve it by an index range scan on `end_time`, and shall not seq-scan or sort `pipeline_executions` (EXPLAIN on 1,000,000 rows, of which 99% are purgeable).
- When `purge_workflow(execution_id)` runs on PostgreSQL, the queue shall find each table's rows by an index on `payload->>'execution_id'` (EXPLAIN on `queue_messages` and `queue_messages_dlq`, each holding 1,000,000 rows).
- When migrations run, the migration shall create the indexes with `IF NOT EXISTS` and provide a `down` section that drops them.
- If a deployment uses additional queue tables, then the documentation shall give the DDL that adds the same expression index to each of those tables.
- The purge of 200 workflows shall take at most 10 s, and 200 lookups at most 5 s, at the sizes above. Changed from 2 s and 0.5 s on 2026-10-30 (stabilize 0.33.3): on the shared CI runner the fixed code took 1.0-2.2 s per purge (pipeline 4366, probe_purge_scales sqlite A 2.207 s against published 0.33.2), while the defect took 26-102 s per purge and 25-79 s for the lookups, so the limits still fail the defect by at least 2.6x and 5x. Index use is asserted exactly by tests/test_purge_query_plan.py; this probe guards against gross regressions.

## TECHNICAL PROBLEMS
1. Ordered range selection over a predicate that matches almost every row.
2. Point lookup on a JSONB field across the queue tables and the DLQ.

## SOLUTION DOMAINS
- PostgreSQL B-tree and expression indexes. Codebase pattern: `idx_queue_messages_stage_id`, a migretti migration.

## ALTERNATIVES
- `pipeline_executions (end_time) WHERE end_time IS NOT NULL`: CHOSEN candidate. It serves both the call with an application filter and the call without one, with `status` checked as a filter.
- `(application, end_time)`: candidate. Better when one application dominates the table, as `ci-v2` does at CI. Decide by EXPLAIN measured on both shapes.
- `(status, end_time)`: REJECTED. `status = ANY(...)` across 4 values breaks the ordered scan into a merge.
- Expression index `((payload->>'execution_id'))` on `queue_messages` and `queue_messages_dlq`: CHOSEN.
- A stored `execution_id` column: REJECTED. It needs a schema change and a backfill on every queue table.

## RESOLUTION (0.33.1)

Chosen: both partial indexes on pipeline_executions, `idx_executions_end_time (end_time)` and
`idx_executions_application_end_time (application, end_time)`, each `WHERE end_time IS NOT NULL`.
The first serves the unfiltered call (CLI without --application) and the second the
application-filtered call. Also expression indexes on `payload->>'execution_id'` for
queue_messages and queue_messages_dlq, which serve purge_workflow and recovery's
has_pending_message_for_execution. That recovery path was found by a class sweep and was not in the report.

SQLite: migration 6 and SqliteQueue._create_table() create the same indexes (plus stage_id).
completed_workflow_ids marks `+status` so that, without ANALYZE statistics, the planner orders by the
end_time index instead of using idx_execution_status and a temp B-tree.

Verification (local gate on vm-1, PostgreSQL 18.6, Python 3.14 and 3.11):
- tests/test_purge_query_plan.py: plans of the product's own SQL, with no Seq Scan, Sort or temp B-tree.
  Red before the fix on both backends; green after. Manual proofs remove an index and turn each leg red.
- probe_purge_scales.py at 1,000,000 rows (purge 200; 200 lookups; limits 2 s and 0.5 s):

  | backend    | call            | 0.33.0   | 0.33.1  |
  |------------|-----------------|----------|---------|
  | postgres   | purge all       | 32.136 s | 0.545 s |
  | postgres   | purge app       | 26.224 s | 0.489 s |
  | postgres   | 200 lookups     | 24.756 s | 0.100 s |
  | sqlite     | purge all       | 102.554 s| 0.049 s |
  | sqlite     | purge app       | 96.287 s | 0.048 s |
  | sqlite     | 200 lookups     | 78.960 s | 0.002 s |

- Gate 62cb95a: lint, tests (2414 passed), probes (61/61), baseline on 0.33.0 (2403 passed, 60/60 probes),
  falsify (1 probe and 1 test red on 0.33.0 for the stated reason), upgrade 0.33.0 -> 0.33.1 with
  work in flight, manual proofs 41/41, soak. Its py311 job failed once on a timing race in
  test_audit_control_matrix; that test was made deterministic in 3219558.
- Gate 5e6f25e (release commit): lint, tests 2414 passed on 3.14 and 3.11, manual proofs 41/41,
  redproof 2407 proven + 40 allowlisted = 2447, 0 violations.

Not tested: index build time and lock duration on a large live table. The migration uses
CREATE INDEX, not CONCURRENTLY, so it takes a write lock on each table while the index builds.
