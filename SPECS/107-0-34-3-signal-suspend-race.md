# #107 — 0.34.3: a persistent signal buffered while the stage suspends on another worker is delivered, not stranded

Ticket: issuedb #107. Reported by casebound-api on 2026-11-03 (thread 01M6G13J3CBC35CCX7DJAPGZ1M) with flow-log evidence from stabilize 0.34.2 on PostgreSQL 18, one QueueProcessor per tenant, max_workers=2: at 05:52:39.408Z the signal handler logged "Buffering persistent signal 'stage_exit' for stage Advice drafted (current status: RUNNING)" and, in the same millisecond, the task path logged "Task Advice drafted suspended, waiting for signal"; the stage stayed SUSPENDED with the row unconsumed until a cancel six minutes later. One stranded of 1346 buffered since 2026-11-02 16:28Z.

## EARS SPEC
- While a stage's task is suspending on one worker, when a persistent signal for that stage is handled on another worker, the engine shall deliver the signal (the stage resumes) rather than leave an unconsumed `workflow_signals` row on a SUSPENDED stage, on PostgreSQL and on SQLite.
- When the signal handler buffers a persistent signal, it shall take the stage row lock inside its transaction, re-read the stage status under that lock, and if the status is SUSPENDED it shall store the row and push the `SignalStage` that delivers it in the same transaction; otherwise it shall buffer as before.
- When the run-task result path suspends a stage, it shall take the stage row lock before it looks for a buffered signal, so a buffer transaction holding the lock commits first and is consumed.
- The transaction interface shall expose `lock_stage(stage_id)` returning the current status name under the lock, implemented for PostgreSQL (`SELECT ... FOR UPDATE`), SQLite (a read inside the `BEGIN IMMEDIATE` transaction, which already serialises writers) and the base class (None, for stores without stages).
- A test shall reproduce the two-worker interleaving deterministically (the signal handler paused between its status read and its buffer transaction while the suspend commits) on both backends, and shall be seen stranding the signal on the 0.34.2 code before the change.
- The 0.34.3 release shall be gated by the full pytest suite on both backends with the published old runners provided, the golden tests and the examples.

## TECHNICAL PROBLEMS
1. Two transactions on different connections racing on one stage's state under READ COMMITTED with no common lock: a check-then-act whose check and act are in different transactions.
2. Delivering a signal from the buffering path without duplicating the SUSPENDED-delivery logic.

## SOLUTION DOMAINS
- PostgreSQL row locks (`SELECT ... FOR UPDATE`) as the serialisation point, the device the engine already uses for claims, admission and recovery; SQLite's `BEGIN IMMEDIATE` writer lock with the status re-read inside it.
- The engine's own `SignalStage(signal_row_id=...)` delivery path, which `recovery_plan.py::_plan_suspended_stage` already uses to deliver a stored row.

## ALTERNATIVES
- Stage row lock in both paths plus a re-read under it [CHOSEN: one serialisation point, no new table, both backends, the existing delivery message].
- Rely on the recovery sweep [REJECTED as the fix: `recovery_interval_seconds` defaults to 0, so the race becomes a strand until a sweep runs; it stays the safety net and the interim mitigation].
- SERIALIZABLE isolation for the two paths [REJECTED: retries everywhere; the engine's transactions are READ COMMITTED by design].

## Synthesis notes
- The pause point in the test is `repository.pending_signal_count`, which the buffer branch calls after reading the status and before opening its transaction; the test replaces it with a function that signals the main thread and waits for the suspend to commit. On the 0.34.2 code the handler then inserts an unconsumed row and marks its message; nothing resumes the stage. With the change the handler locks the stage, reads SUSPENDED, stores the row and pushes `SignalStage(signal_row_id=…)`; draining the queue resumes the task, which succeeds with the signal's data.
- Interim mitigation sent to Casebound: set `recovery_interval_seconds` so the sweep delivers a stranded signal within one interval.
- The SQLite case of the test uses a file-backed store and queue in `tmp_path`, because the fixture's in-memory database is per connection and the signal handler runs on a second thread.

## Done (2026-11-03)
- Red on the 0.34.2 code, both backends: `AssertionError: stage left SUSPENDED with the signal stranded`. Green with the change: 2 passed; the 31 existing signal tests pass. ruff and mypy clean.
- First gate run (fullsuite-6): 2971 passed, 2 failed, neither in the fix. (1) The 500-line cap: `lock_stage` took `persistence/postgres/transaction.py` to 510 lines; the signal methods and the lock moved to `persistence/postgres/transaction_signals.py` (`PostgresSignalTransactionMixin`), the transaction module is 411 lines. (2) `tests/test_subscription_cursor.py` "latest" test asserted exactly `[4]`; under three parallel workers on one cluster it saw `[2, 3, 4]`, because the commit cursor is `pg_snapshot_xmin(pg_current_snapshot())`, which is cluster-wide: a transaction open on another worker's database while events 1 to 3 committed makes them "not yet certain" at creation and the at-least-once contract re-delivers them. Passed 3 of 3 alone. The test now asserts the contract: the new event arrives once, no gap in commit order, and history replays only on a store with a commit cursor. Not an engine defect; the documented at-least-once behaviour of durable subscriptions.
- Second gate run (fullsuite-7, no edits during the run): 2973 passed, 1 skipped (the SQLite-only demonstration, by design), 0 failed, 7 min 56 s, both backends, published 0.33.1 and 0.32.1 runners provided, golden tests and headless examples inside the run.
- Release 0.34.3 built from `git archive` of the release commit with `SOURCE_DATE_EPOCH`; commit, tag and digests recorded on the ticket after upload.
