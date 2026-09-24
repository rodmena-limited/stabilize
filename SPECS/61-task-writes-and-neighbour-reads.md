# 61 — Write only changed task rows; neighbour lookups read only neighbours

Ticket: #61 (released in 0.31.0; split from #54)

## EARS spec

- When a stage is stored, `store_stage` shall write only the task rows whose state differs from their row, on PostgreSQL and SQLite.
- If a writer holding a stale stage stores it, then `store_stage` shall raise `ConcurrencyError` whether or not it changed a task, and a writer that reloads shall succeed.
- If a stage write fails or its transaction rolls back, then the stage's and tasks' in-memory versions shall equal their values before the write, and a task change that did not commit shall be written by the next store.
- When a stage row is inserted, all of its task rows shall be written.
- `get_upstream_stages`, `get_downstream_stages` and `get_synthetic_stages` shall not read the workflow row or the workflow's full stage list.
- Each queue message shall read the workflow row at most once, except RunTask's re-read after its task has run.
- `tests/test_query_budget.py` shall hold the measured bounds.

## Synthesis

- Technical problems: (a) redundant task writes on every stage write; (b) redundant workflow reads per neighbour lookup.
- Solution domain: ORM unit of work and dirty checking with optimistic versioning (Hibernate/JPA increments `@Version` only for dirty entities; SQLAlchemy flushes only changed rows). The stage is the aggregate: every task write goes through `store_stage`, whose stage-version UPDATE runs first in the same transaction.
- Alternatives for (a):
  - Per-task snapshot of the persisted state, set on load and after commit [CHOSEN].
  - Store-level identity map keyed by task id [REJECTED: unbounded memory for the process lifetime and shared mutable state across threads].
  - Compare against a fresh read [REJECTED: costs the statement it saves].
- Alternatives for (b):
  - Stop loading the workflow in neighbour lookups [CHOSEN]: it was attached by weak reference only and collected before the call returned (CONFIRMED on both backends: `has_execution()` False on every returned neighbour), so nothing could observe it.
  - Attach it strongly [REJECTED: turns an O(N) waste into an O(N) feature nobody used].
  - Per-message identity map [REJECTED: an architecture change with its own staleness rules, unnecessary once the waste is gone].

## Design notes

- The snapshot (`TaskExecution._persisted_state`, not an init/compare/repr field) holds id, version and every persisted field. It is captured at write time and applied only after commit, so a rollback, or a change made between the write and the commit, never marks unwritten state as persisted.
- Skipping happens only on the update path, after the stage row is proven to exist; insert paths always write every task.
- Transactions restore each task's exact prior version on rollback (the PostgreSQL transaction previously assumed `version - 1`).
- The non-transactional `store_stage` restores versions on failure; on SQLite it also rolls the connection back instead of leaving a partial write open.

## Measured (PostgreSQL 16)

| | 0.30.4 | 0.31.0 |
|---|---|---|
| one stage | 97 | 87 |
| each extra stage | 87 | 67 |
| task INSERT per stage | 7 | 3 |
| workflow-row reads, 2 stages | 16 | 14 |

## Evidence

`tests/test_task_dirty_writes.py`, `tests/test_neighbour_lookup_cost.py`, both backends.

- 0.30.4 code: the 10 behaviour tests fail for the stated reasons (task versions bumped on an unchanged write; `pipeline_executions` read; unfiltered stage list read; stage version left bumped after a failed write).
- Guard tests (change and revert, rollback, change after write before commit, stale writer with and without task changes, re-add after removal, counter control) pass on 0.30.4 and 0.31.0.
