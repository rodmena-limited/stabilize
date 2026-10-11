# Spec: Fix runflow evaluation findings 1, 2, 8, 9, 10 (+ processed_messages retention)

**Ticket:** issuedb #9 · **Date:** 2026-07-30 · **Origin:** runflow report, thread `thr-eb29f7cc40dd4a098f7e`

## EARS requirements

- When `Orchestrator.cancel`/`restart`/`unpause` push `CancelWorkflow`/`RestartStage`/`ResumeStage`, the processor shall execute them via registered handlers (cancel terminates the workflow as CANCELED; restart re-runs a terminal stage; resume re-arms a parked task), never consume-and-discard them.
- When `RunTaskHandler` parks an in-flight task as `PauseTask` on a paused workflow, the task shall be persisted as PAUSED and be runnable again after resume, not lost.
- If a message arrives with no registered handler, then the processor shall raise (retry then DLQ) instead of acking, so lost instructions are loud.
- While the processor runs as a daemon (`start()`), poisoned messages shall be swept to the DLQ periodically (`dlq_check_interval_seconds`), and the weak-spot test shall exercise the live sweep in both directions.
- The distribution shall declare `resilient-circuit` as a direct dependency.
- CI shall run the full non-postgres suite (not `-k sqlite`) so unparametrized tests (incl. audit certification) are continuously enforced.
- CLAUDE.md shall not claim a nonexistent In-Memory store nor WAL-by-default.

## Verification

- `tests/test_workflow_control.py` (6 tests): live cancel-mid-run via threaded processor; cancel-before-start; idempotent late cancel; restart-to-success; pause/park/resume roundtrip; unregistered-message-not-acked guard.
- `tests/test_weak_spots.py::TestPoisonMessageToDLQ`: live-processor sweep green case + sweep-disabled red case (both directions).
- Full suite (SQLite + Postgres) re-run after changes; ruff + mypy clean.
