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

## Phase 4 design refutation (fresh-context refuter, 2026-10-11) — amendments

- B84-1 (false SUCCEEDED): the CompleteStage heal requires the stage to have tasks or synthetic stages; a task-less, synthetic-less RUNNING stage keeps the StartStage (zombie re-plan) path.
- B84-2 (stale marker): marker keyed by (task_id, task.start_time) and used only on an exact match; cleared on CompleteTask's ignore-duplicate path, in reset_stage_for_retry, and not restored by JumpToStage's context snapshot; registered in engine_keys / HIDDEN_FROM_TASK_INPUT.
- B84-3 (duplicate execution): every completion producer writes the marker on the stage object it stores: _handle_success_like/_terminal/_canceled/_unhandled, _mark_terminal and complete_with_error (on fresh_stage), handle_cancellation, the RunTask manual-skip and execution-complete paths (which gain a store_stage).
- B84-4 (stranding): CompleteWorkflow heal when no top-level stage is active and none is startable (covers a TERMINAL stage with NOT_STARTED downstream), not only "all complete".
- C7 sweep: ResumeStage (workflow_control.py:307-313) and SignalStage (signal_stage.py:111-116) also push RunTask without task_type; both set it.
- has_pending_message_for_stage matches any message with payload stage_id = the stage; PostgreSQL gets an expression index migration on payload->>'stage_id' (supersedes "no schema change" in this ticket).

## Round 2 (pipeline 2936)

N2 CONFIRMED (probe_synthetic_stage_paths.py A, both backends): a recovery sweep while a STAGE_BEFORE synthetic stage was polling pushed StartTask for the parent's first task; the parent's task ran before the before-stage ended, and the parent stayed RUNNING. EARS: recovery shall push StartTask for a RUNNING stage's first NOT_STARTED task only when every before-stage of that stage is complete; otherwise it shall leave the stage to the before-stages' ContinueParentStage.

## Implementation (commit 8f40cad) — callers

TransactionHelper.execute_atomic / execute_atomic_critical now record a completion on the stage they store for every CompleteTask they push to that stage. Producers (grep): run_task/result.py:99 (running: no CompleteTask), :132 redirect, :169 success-like, :235 suspended-resume (RunTask only), :262 canceled, :280 terminal, :309 suspended (none), :344 unhandled; run_task/handler.py:193 execution-complete and :234 manual skip (now pass stage), :214 pause (PauseTask, no record); run_task/error.py:78 stage-missing (no stage: nothing to record), :84 cancellation (fresh stage), :218/:227/:241 transient paths (RunTask re-queue), :290/:386 stage-missing critical paths, :332 _mark_terminal and :409 complete_with_error (fresh stage). continue_parent_stage.py execute_atomic calls push CompleteStage/StartTask, not CompleteTask: no record. CompleteTask clears the record in its transaction. Recovery uses it only on an exact (task id, start_time) match.
