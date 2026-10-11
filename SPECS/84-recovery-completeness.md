# 84 — Recovery re-issues the exact lost message: task type, pending completion, stage and workflow completion (C7, C32, C33, C34)

Ticket: #84. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (pipeline 2930). C7: after a lost RunTask, the recovered poll ran in Bulkhead-stabilize_default_0 instead of Bulkhead-stabilize_job_0. C33: a lost CompleteTask made recovery push RunTask and the task executed twice. C32: a lost CompleteStage left the workflow RUNNING with every task SUCCEEDED. C34: a lost CompleteWorkflow left the workflow RUNNING with every stage SUCCEEDED. Probes: probe_recovered_runtask_type.py, probe_lost_message_recovery.py. Peer v3 #2, item E.

## EARS spec

- When recovery re-issues RunTask, it shall set task_type to the task's implementing_class.
- When RunTask commits a completing result, it shall record the pending completion (task id, status, original status) on the stage in the same transaction, and CompleteTask shall clear it in its transaction.
- When recovery finds a RUNNING task with a recorded pending completion and no queued message for it, it shall push that CompleteTask, not RunTask.
- When recovery finds a RUNNING stage whose tasks are all complete and no queued message for the stage, it shall push CompleteStage.
- When recovery finds a RUNNING workflow whose top-level stages are all complete and no queued message for it, it shall push CompleteWorkflow.
- For every single lost lifecycle message, one recovery sweep shall finish the workflow with the task executed exactly once (probe_lost_message_recovery, 8 cases, both backends).

## Technical problems

1. Reconciliation from durable state: recovery must derive the missing message from what the store already records; a completion whose only record was the lost message is invisible.
2. Re-issued messages must equal what the normal path would have produced (task_type).

## Solution domains

- Transactional outbox / state reconciliation: every message must be reconstructible from committed state.
- Codebase: recovery.py _recover_workflow; queue.has_pending_message_for_task / _for_execution.

## Alternatives

- Pending completion: reserved stage-context key written by RunTask's commit [CHOSEN: no schema change; committed atomically with the result] vs new task column [REJECTED: migration on both backends for one field] vs search the DLQ for the lost CompleteTask [REJECTED: a purged DLQ loses it, the case we must cover] vs leave as RunTask re-execution [REJECTED: duplicate side effect, confirmed].
- Stage/workflow completion: recovery pushes CompleteStage / CompleteWorkflow guarded by a per-stage / per-execution pending check [CHOSEN] vs CompleteWorkflow bounded retry as the net (current) [REJECTED: it does not exist for a lost CompleteStage, and #86 removes its false TERMINAL].

## Callers (contract change)

- recovery._recover_workflow is reached from WorkflowRecovery.recover_pending_workflows <- QueueProcessor.run_recovery / _recovery_loop, recover_on_startup. New queue method has_pending_message_for_stage on Queue, PostgresQueue, SqliteQueue (+ in-memory if present).

## Die-here / two-at-once

- Recovery sweep racing a live CompleteTask: the pending-message checks include in-flight (locked) rows, so a message being handled is pending.
- Pending completion recorded, CompleteTask handled, die before ack: redelivery is a dedup skip; the key was cleared in CompleteTask's transaction.
