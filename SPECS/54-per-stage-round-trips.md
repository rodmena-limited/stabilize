# 54 — Remaining per-stage round trips

Ticket: #54 (released in 0.31.0). Remainder delivered in the same release as #61.

## EARS spec

- When store_stage writes an existing stage, it shall not issue a separate existence query before its optimistic-lock UPDATE.
- When retrieve_stage loads a stage, it shall load upstream and synthetic stages on the connection it already holds, in one statement.
- If store_stage's UPDATE matches no row, then it shall INSERT when the stage does not exist and raise ConcurrencyError when it does.
- Each change shall lower tests/test_query_budget.py's bounds to the measured count.

## Measured (PostgreSQL 16, audit/evaluations/measure_queries_per_stage.py)

| | 0.30.4 | 0.31.0 |
|---|---|---|
| one stage | 97 | 91 |
| each extra stage | 87 | 75 |

## Changes

- `store_stage`: UPDATE ... RETURNING version first; `SELECT id` only when that matched nothing, to choose INSERT vs ConcurrencyError. Same outcomes as before for all three cases (updated, missing, conflicting).
- `retrieve_stage`: one `SELECT ... WHERE execution_id AND (ref_id = ANY(requisites) OR parent_stage_id = id)` on the held cursor replaces the separate upstream query and `get_synthetic_stages`, which borrowed a second pool connection while the first was held.

## Evidence

- `tests/test_retrieve_stage_single_connection.py` on a pool of max_size 1: red on 0.30.4 (`PoolTimeout` at the nested `get_synthetic_stages` borrow), green after.
- `tests/test_query_budget.py` bounds lowered to 91 / 75.

## Not done, and why (moved to #61)

- `store_stage` rewrites task rows on every write (7 statements per stage). The rewrite bumps each task's version, which is what makes a concurrent task writer fail its optimistic lock; skipping it changes concurrency behaviour.
- The workflow row is read 9 times per stage: each message handler loads it independently; removing reads needs a per-message cache with its own staleness rules.

## Class sweep

Other `with pool.connection()` blocks calling a second borrow: `PostgresQueue.poll_one` -> `move_to_dlq`. That branch is effectively unreachable today and sits behind a larger defect, filed as #60.
