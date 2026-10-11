# Specs: runflow evaluation backlog (tickets #4–#8)

**Origin:** agent-mail report from runflow, thread `thr-eb29f7cc40dd4a098f7e` (2026-07-30).
Findings 1, 2, 8, 9, 10 and the processed_messages half of 3 were fixed in the
same pass (see tickets #1 and git history); these five are the tracked remainder.

## Ticket #4 — Submit-time DAG validation in Workflow.create (finding 4)

- When `Workflow.create()` receives stages, the engine shall validate the stage graph (cycles incl. self-edges, unknown `requisite_stage_ref_ids`, duplicate ref_ids) and raise a clear error naming the offending stages, instead of discovering the defect at runtime as "Exceeded max retries waiting for upstream stages".
- The cycle error shall name the cycle members.
- Where conditional execution semantics exist, the validator should flag nodes that can never run under any assignment of upstream terminal states (condition-aware reachability).

## Ticket #5 — Recovery config not carried by from_handler_config (finding 5)

- When `QueueProcessor` is constructed without an explicit config, `QueueProcessorConfig.from_handler_config` shall carry recovery settings (`recover_on_start`, `recovery_interval_seconds`) or the processor shall log a startup warning that crash recovery is disabled.

## Ticket #6 — has_pending_message_for_task full-scan LIKE (finding 6)

- When recovery checks for a pending message for a running task on PostgreSQL, the query shall use an indexable predicate (extracted task_id column or JSONB expression index) instead of a leading-wildcard LIKE over `payload::text`, so recovery cost stays sub-linear in queue depth.

## Ticket #7 — Thread-mode timeouts are soft (finding 7)

- While `STABILIZE_ISOLATION_MODE` is thread (default), when a task exceeds its timeout, the engine shall log a warning that the worker thread keeps running (leaked) and that process mode enforces hard kills.
- The resilience docs shall state prominently that thread-mode timeouts are soft and a hung task occupies a worker slot until it returns.

## Ticket #8 — stage_claims retention sweep (finding 3, second half)

- When a retention sweep runs, the engine shall delete `stage_claims` rows only for executions in terminal states, so a claim belonging to a live workflow is never removed (removing one would resurrect the WCP-16/17 mutex/deferred-choice race the claim exists to prevent).
- The sweep shall be exercised in both directions by tests: claims of completed executions removed; claims of running executions retained.
