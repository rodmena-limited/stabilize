# 78 — A failed task stops its stage: no later task of the stage starts (C0)

Ticket: #78. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (CI pipeline 2930, falsify job 8484): side effect executed after TERMINAL, raised exception, STOPPED, CANCELED and continue-on-failure TERMINAL, 10/10 cases on SQLite and PostgreSQL. Probe: audit/evaluations/probe_task_after_failure.py. Test: tests/test_task_after_failure.py.

## EARS spec

- When a task completes with TERMINAL, STOPPED or CANCELED, the CompleteTask handler shall not start any later task of the same stage, and shall push CompleteStage in the same transaction as the task's status.
- When a task completes with FAILED_CONTINUE whose original status is TERMINAL (continuePipelineOnFailure converted a failure), the CompleteTask handler shall not start any later task of the stage.
- When a task itself returns FAILED_CONTINUE (original status absent or FAILED_CONTINUE), or completes SUCCEEDED or SKIPPED, the CompleteTask handler shall start the next task, as before.
- When a stage is completed early by these rules, every later task that has not started shall be set SKIPPED with no start_time in the same transaction, so the stage's derived status is final (no NOT_STARTED task keeps it RUNNING).
- The probe and the regression test shall report 0 executions of the later task in all 5 failure cases on both backends, and 1 in the success control.

## Technical problems

1. Sequential composition with fail-stop semantics inside a stage: the decision "continue or halt" depends on the completed task's outcome and on whether a FAILED_CONTINUE is the task's own verdict or a policy conversion.
2. Leaving the stage's derived status final when it ends before its last task.

## Solution domains

- Workflow semantics of the engine stabilize is modelled on (Netflix Orca CompleteTaskHandler.shouldCompleteStage): halt on anything but SUCCEEDED, except a FAILED_CONTINUE the task returned itself.
- Codebase: CompleteTask already carries original_status (set by _handle_terminal, _handle_canceled, _mark_terminal) and never reads it; VALID_TRANSITIONS allows NOT_STARTED -> SKIPPED.

## Alternatives

- Halt rule: in CompleteTask, continue only on SUCCEEDED / SKIPPED / task-returned FAILED_CONTINUE [CHOSEN: one decision point, uses the field already carried] vs make RunTask push CompleteStage directly on failure [REJECTED: CompleteTask's status write and event would be skipped, and REDIRECT/SKIPPED paths would need a second rule] vs StartTask refusing to start after a failed sibling [REJECTED: the wrong message would still be pushed; a recovery sweep could re-issue it].
- Remaining tasks: set SKIPPED [CHOSEN: determine_status treats SKIPPED as complete; NOT_STARTED makes a FAILED_CONTINUE stage read RUNNING] vs leave NOT_STARTED [REJECTED: wedges the converted FAILED_CONTINUE stage in CompleteStage] vs CANCELED [REJECTED: determine_status would turn a FAILED_CONTINUE stage into CANCELED and stop the pipeline].

## Callers (contract change: what CompleteTask pushes)

- `stage.next_task(` has one caller, handlers/complete_task.py. Producers of CompleteTask(status, original_status): run_task/result.py (_handle_success_like: no original_status; _handle_terminal / _handle_canceled: original set), run_task/error.py (_mark_terminal: original TERMINAL; complete_with_error and stage-missing paths: TERMINAL with no original -> halts, correct), start_task.py (SKIPPED -> continues, correct), run_task handler manual skip (SKIPPED), execution-complete path (CANCELED -> halts, correct).

## Die-here / two-at-once

- Die after the CompleteTask transaction: task status, SKIPPED siblings and CompleteStage commit together; nothing half-done.
- Redelivered CompleteTask: task no longer RUNNING -> ignored and marked processed (existing guard).
- Recovery sweep racing: the stage now has no RUNNING / NOT_STARTED task; recovery's StartStage on a RUNNING stage with tasks is ignored, and the queued CompleteStage completes it.

## Class sweep

- Other "continue after failure" points: CompleteStage starts downstream only for SUCCEEDED / FAILED_CONTINUE / SKIPPED (correct); after-stages vs on-failure stages are planned by status (correct); ContinueParentStage and JumpToStage do not start sibling tasks. No second instance found.

## Phase 4 design refutation (fresh-context refuter, 2026-10-11) — amendments

- B78-1 (duplicate execution): _handle_canceled with continuePipelineOnFailure sends CompleteTask(FAILED_CONTINUE, original=CANCELED); _handle_unhandled sends original = the odd status. The halt rule is therefore stated positively: CONTINUE iff status in {SUCCEEDED, SKIPPED} or (status == FAILED_CONTINUE and original_status in {None, FAILED_CONTINUE}); everything else halts.
- B78-2: halted later tasks are set SKIPPED with end_time = now (start_time None), so they are distinguishable from the #80 acceptance state (SKIPPED, end_time None).
