# 16 — Move WCP-24 persistent-signal buffering to the workflow_signals table

Ticket: issuedb #16 · status in-progress · priority medium

EARS SPEC:
- When a persistent SignalStage message is buffered, the engine shall write one row to the workflow_signals table rather than appending to stage_executions.context.
- When a stage enters SUSPENDED, the engine shall consume the oldest matching unconsumed row from workflow_signals rather than popping from stage context.
- The buffering write shall be atomic with mark_message_processed for the source message, so a signal is never both buffered and redelivered, nor lost.
- Where a deployment carries existing _buffered_signals entries in stage context, the engine shall read from both locations for one release cycle and write only to workflow_signals, so no buffered signal is lost at upgrade.
- The SQLite backend shall create the workflow_signals table, which create_signals_table currently never does.
- The engine shall remove consumed and orphaned workflow_signals rows when their execution reaches a terminal state.

TECHNICAL PROBLEMS:
1. Storage shape: a row-per-signal store is indexable, individually deletable, and costs O(1) per append, against O(n) re-serialisation of one JSONB document.
2. Atomicity: the buffer write must join the existing store+queue transaction.
3. Backend parity: sqlite, postgres and memory stores must agree.
4. Upgrade safety: existing contexts hold entries that must not vanish.

SOLUTION DOMAINS:
- Inbox/outbox row-per-message buffering -> already instantiated in this repo as the workflow_signals table (migration 01KGHWCP1M2QSDE3TN4GUBLV8A) with a complete unused SQLite implementation at src/stabilize/persistence/sqlite/signals.py. Extract, do not reinvent.
- Dual-read migration -> the standard expand/migrate/contract pattern.

ALTERNATIVES: to be recorded when this ticket starts. At minimum: dual-read/single-write vs a one-shot backfill migration; and signal_name-matched consumption vs the current FIFO pop, which today resumes a task with whatever signal is oldest regardless of what it was waiting for.

CONTEXT: split out of #15. #15 bounds the leak completely wherever the bytes live; this ticket makes the storage structurally right. Deliberately not shipped in the same release as the #15 incident remediation because it touches the hot path of every suspended stage. ci-conductor-dd94c8 agreed with the split and confirmed workflow_signals has zero rows in production.

BLOCKED BY: #15

## Progress, evidence and synthesis changes

STARTED, WITH THE PROBE FIRST THIS TIME. One of the four EARS lines is now closed; the substantive half is not.

THE PROBE WAS WRITTEN BEFORE ANY FIX AND CONFIRMED RED, so its failure is the defect rather than a regression: audit/evaluations/probe_signal_buffer_storage.py, VERDICT: FAIL - 3 of 4, with the control passing so it is not vacuous.

    [FAIL] workflow_signals table is created: ['pipeline_executions','processed_messages','queue_messages','queue_messages_dlq','schema_migrations','sqlite_sequence','stage_claims','stage_executions','task_executions']
    [PASS] table census is working: 9 tables
    [FAIL] store surfaces signal buffering: []
    [FAIL] the helper is wired, not dead code: exists=True created=False

CONFIRMED: workflow_signals was absent from every table a fresh SQLite store creates. persistence/sqlite/signals.py has create_signals_table, buffer_signal, consume_signal, get_pending_signals and cleanup_consumed_signals, and NOTHING CALLED ANY OF THEM. The whole module was dead code, so on SQLite the table did not exist at all while PostgreSQL got it from migration 01KGHWCP1M2QSDE3TN4GUBLV8A.

FIXED IN THIS PASS: create_signals_table is now called from persistence/sqlite/schema.py create_tables(), before apply_migrations. The probe goes 3 failures to 1. That closes the EARS line 'The SQLite backend shall create the workflow_signals table, which create_signals_table currently never does.'

STILL OPEN, and it is the larger half:
- The store does not surface buffering through its interface. The helpers take a raw sqlite3.Connection, so they are SQLite-only and unreachable from handler code, which goes through WorkflowStore. PostgreSQL has the table but no helpers at all.
- Buffering still writes stage_executions.context['_buffered_signals'] at handlers/signal_stage.py:135,152; consumption pops it at handlers/run_task/result.py:208,212; discard at handlers/base.py:481.
- Dual-read for existing context entries is not started.
- Atomicity with mark_message_processed is not started.

WHAT THE PROBE DELIBERATELY DOES NOT ASSERT, printed in its own output so nobody infers coverage it lacks: end-to-end signal delivery through the suspend/resume path. Without that check, an engine that dropped signals entirely would satisfy every assertion in the probe. It must be added in the same change as the storage move, before this ticket closes.

SEQUENCING NOTE: this ticket is no longer coupled to issue 37. The _engine envelope that would have held _buffered_signals was withdrawn, so there is nothing for it to move into or out of - it moves from context to the table directly.
