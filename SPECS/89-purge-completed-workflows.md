# 89 — Purge completed workflows in batches through the product interface (C15, peer v3 #7b)

Ticket: #89. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: feature gap reported by ci-conductor (v3 #7b, with #143). No product interface removes completed workflows in bulk; delete(execution_id) (#67) removes one workflow and its owned rows, and Queue.purge_workflow(execution_id) its queued and dead-lettered messages.

## EARS spec

- The store shall provide purge_completed_workflows(completed_before_ms, batch_size=500, max_workflows=None, application=None) that deletes, through delete(), only workflows whose status is complete (SUCCEEDED, FAILED_CONTINUE, TERMINAL, CANCELED, STOPPED, SKIPPED) and whose end_time is before completed_before_ms, in batches of batch_size, and returns the number deleted.
- It shall never delete a workflow that is NOT_STARTED, RUNNING, PAUSED, SUSPENDED or BUFFERED, or one with no end_time.
- stabilize.maintenance.purge_completed_workflows(store, queue, ...) shall also call queue.purge_workflow(id) for each deleted workflow, so its dead-lettered messages go with it.
- The CLI shall offer `stabilize purge-workflows --db-url URL --completed-before-days N [--application A] [--queue-table T] [--batch-size N] [--dry-run]`, printing the count; --dry-run prints the count only.
- Event-store rows are kept (the audit trail, as decided in #67).

## Technical problems

1. Bulk retention of terminal aggregates with their owned rows, bounded per transaction, never touching live work.

## Solution domains

- Retention by terminal timestamp (end_time), batched; reuse of the aggregate delete that already knows the owned rows (#67).

## Alternatives

- Reuse delete() per workflow in batches [CHOSEN: one definition of "owned rows"] vs one SQL DELETE ... USING with cascades [REJECTED: duplicates #67's owned-row list; processed_messages and signals are not FK-cascaded by design] vs start_time-based cutoff [REJECTED: a long-running workflow completed yesterday would qualify by its start].

## Callers

- New API; no existing caller changes.
