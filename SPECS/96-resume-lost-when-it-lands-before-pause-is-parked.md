# 96 — A resume that lands before PauseTask parks the task is lost; the workflow stays RUNNING with a PAUSED stage forever

Ticket: issuedb #96. Found 2026-10-30 from CI pipeline 4346 (jobs 16489 and 16492): `tests/test_audit_control_matrix.py::test_pause_while_a_stage_runs_and_its_successor_has_not_started[postgres]` failed on this branch and on published 0.33.1 alike with `workflow RUNNING, stages {'a': 'PAUSED', 'b': 'NOT_STARTED'}`. It passed in pipelines 4342 and 4343 on the same engine code, so it depends on timing.

## Cause

Pausing a running stage takes two messages. `RunTask` sees the workflow PAUSED and pushes `PauseTask`; `PauseTask` then marks the task and stage PAUSED. `Orchestrator.unpause` resumes only stages that are already PAUSED, chosen from a read taken outside its transaction. When the resume arrives between the two messages, `unpause` finds no paused stage, sets the workflow RUNNING, and `PauseTask` then parks the stage anyway. Nothing resumes it again.

## EARS spec
- R1. If the workflow is no longer PAUSED when `PauseTask` is handled, then the engine shall re-dispatch the task with `RunTask` and shall not mark the task or stage PAUSED.
- R2. When `unpause` runs, the engine shall decide which stages to resume from a read made inside the same transaction that pushes the resume messages.
- R3. While `PauseTask` or `unpause` holds the per-workflow control lock, the other shall wait, on PostgreSQL (transaction advisory lock, 30 s lock timeout) and SQLite (`BEGIN IMMEDIATE`).
- R4. Where a store's transaction is not atomic, `unpause` and `PauseTask` shall keep their previous behaviour.
- R5. A test shall force the resume to land between `RunTask` and `PauseTask` on both backends, through the real handlers, and shall observe the workflow SUCCEEDED with no stage left PAUSED. It shall fail on published 0.33.1 for that reason.

## Technical problem
Lost update between two transactions that each check state and then act on it (check-then-act across transactions).

## Solution domain
Mutual exclusion with re-check under the lock (PostgreSQL manual, advisory locks; SQLite `BEGIN IMMEDIATE`). Codebase pattern: admission control already serializes per key with `txn.lock_admission(key)` and reads state with `txn.workflow_status`.

## Alternatives
- Per-workflow control lock taken by both `PauseTask` and `unpause`, with state re-read under it [CHOSEN]: whichever commits second sees the other's result, so the stage is either parked and then resumed, or never parked.
- Compare-and-set in `PauseTask` only (park only if the workflow is still PAUSED) [REJECTED: `unpause` still chooses stages from a stale read, so a park that commits between its read and its write is still lost].
- Have `unpause` also re-dispatch RUNNING stages [REJECTED: it would run a task that is already executing a second time].
- Retry `unpause` until no stage is PAUSED [REJECTED: a poll loop on the caller's thread with no correct bound].
