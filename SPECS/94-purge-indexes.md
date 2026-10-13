# #94 Purge queries use indexes (reported by ci-conductor, thread 01M4SV63YZPAH8TVZK1D4NBBQW)

Status: SUSPECTED cost defect. No index on pipeline_executions includes end_time, and no queue table has an execution_id index (confirmed from migrations/). Existing pipeline_executions indexes: application, pipeline_config_id, status, created_at DESC, (application, status), (pipeline_config_id, status).

Measured by ci-conductor on pg-nano-03 (2026-10-13 05:22Z, 2781 rows): the application-filtered pass used idx_execution_application_status, 0.236 ms execution, 11 shared buffers. Trivial at that size; the cost at 1 M rows is unmeasured.

## EARS SPEC
- When `completed_workflow_ids` runs on PostgreSQL, the store shall resolve it by an index range scan on `end_time`, and shall not seq-scan or sort `pipeline_executions` (EXPLAIN on 1,000,000 rows, of which 99% are purgeable).
- When `purge_workflow(execution_id)` runs on PostgreSQL, the queue shall find each table's rows by an index on `payload->>'execution_id'` (EXPLAIN on `queue_messages` and `queue_messages_dlq`, each holding 1,000,000 rows).
- When migrations run, the migration shall create the indexes with `IF NOT EXISTS` and provide a `down` section that drops them.
- If a deployment uses additional queue tables, then the documentation shall give the DDL that adds the same expression index to each of those tables.
- The purge of 200 workflows shall take at most 2 s at the sizes above (to be measured).

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
