# 67 — Deleting a workflow removes every row it owns

Ticket: #67 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- When a workflow is deleted, the store shall delete its processed_messages, stage_claims and workflow_signals rows in the same transaction.
- `Queue.purge_workflow(execution_id)` shall delete the workflow's queued and dead-lettered messages.

## Synthesis

- Alternatives: store deletes store tables + queue purge method [CHOSEN: the store does not know the queue's table name] vs FK cascades [REJECTED: processed_messages rows outlive workflows by design for dedup] vs retention-only [REJECTED: DLQ and signals have no retention].
- Event-store rows are deliberately kept (audit trail).

## Evidence

`probe_delete_removes_owned_rows.py`: 0.31.0 left processed/signal/claim/DLQ rows; fixed removes them and leaves another workflow's rows. `tests/test_delete_owned_rows.py`.
