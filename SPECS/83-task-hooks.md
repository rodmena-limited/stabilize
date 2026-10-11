# 83 — Raising task hooks end their task once; cancel runs on_cancel and on_cleanup (C11, C12)

Ticket: #83. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (pipeline 2930). C11: a raising on_timeout was called 10 times (once per redelivery) and its task left RUNNING; a raising is_enabled was called 10 times and its task left NOT_STARTED; a returning on_timeout control ended TERMINAL with 1 call. C12: canceling a workflow while its RetryableTask polled called on_cancel 0 times and on_cleanup 0 times; a completing control called on_cleanup once. Probes: probe_raising_task_hooks.py, probe_cancel_runs_task_hooks.py. Peer v3 #3, #4, item D.

## EARS spec

- If on_timeout raises, then the task shall complete TERMINAL with an error naming both the timeout and the hook's exception, and on_timeout shall have been called once.
- If is_enabled raises at StartTask, then the task shall complete TERMINAL with the exception recorded, and is_enabled shall have been called once; the task does not execute.
- If on_cancel raises, then the task shall still complete CANCELED.
- When CancelStage moves a RUNNING task to CANCELED, it shall call that task's on_cancel once, best-effort and logged.
- When CancelStage cancels a stage, it shall call on_cleanup for every task of the stage once, best-effort and logged, as CompleteStage already does on other terminal states.
- The probes shall show hook call count 1 and a terminal task in every case on both backends.

## Technical problems

1. User code invoked from inside a handler without an exception boundary, so its failure becomes a handler failure (redelivery loop) instead of a task outcome.
2. A lifecycle hook only reachable on one interleaving (RunTask's is_canceled branch), unreachable when CancelStage runs first.

## Solution domains

- Exception boundary around plug-in callbacks, converting a raise into the domain outcome (the same as CompleteStage's _invoke_task_cleanup).
- Exactly-once hook invocation tied to the state transition that defines the event (RUNNING -> CANCELED).

## Alternatives

- on_cancel placement: CancelStage on its RUNNING -> CANCELED transition [CHOSEN: the transition happens once, so the hook does] vs RunTask only (current) [REJECTED: unreachable when CancelStage is first, confirmed 0/6 by the peer and 0 here] vs both [REJECTED: double call when RunTask's is_canceled branch runs first and then CancelStage — guarded because CancelStage only calls it for tasks it moves from RUNNING].
- is_enabled raising: task TERMINAL (fail closed: do not run) [CHOSEN] vs treat as enabled [REJECTED: runs a task whose gate could not be evaluated].

## Callers (contract change)

- CancelStageHandler gains an optional task_registry: constructed in queue/processor/mixins.py _register_default_handlers and handlers/__init__.py (to be listed with grep). Without a registry the hooks are skipped and a warning is logged once.
- Task.on_cancel semantics: now called by CancelStage; RunTask's handle_cancellation keeps calling it for the interleaving where it sees is_canceled first, which leaves the task CANCELED, so CancelStage does not call it again.

## Die-here / two-at-once

- Hook runs, process dies before the CancelStage transaction commits: redelivery calls the hook again (at-least-once for hooks across a crash; documented).
- A RunTask executing in another process while CancelStage runs: on_cancel runs concurrently with execute; the cooperative token is set in-process only. Documented.

## Phase 4 design refutation (fresh-context refuter, 2026-10-11) — amendments

- B83-1 (on_cancel twice): handle_cancellation leaves the task RUNNING until CompleteTask(CANCELED) is handled; CancelStage in that window calls on_cancel again. Design: handle_cancellation sets the task CANCELED with end_time in its own transaction (and pushes CompleteTask as before); CancelStage calls on_cancel only for tasks it moves from RUNNING.
- B83-2 (hooks repeat on version conflict): CancelStage computes the moved-task set inside its transaction and calls on_cancel/on_cleanup after commit, outside retry_on_concurrency_error: at-most-once per transition (documented).
- B83-3 (gate-raised task executes after a lost CompleteTask): StartTask's is_enabled-raised path writes the #84 pending-completion marker and task_exception_details in the same transaction, so recovery pushes CompleteTask(TERMINAL), never RunTask.

## Round 2 (pipeline 2936)

N4 CONFIRMED (probe_cleanup_once_on_conflict.py, both backends): one injected ConcurrencyError at stage completion -> on_cleanup called 2 times (control 1). Class: CompleteStage calls _invoke_task_cleanup and run_stage_finalizers before its transaction, inside retry_on_concurrency_error (complete_stage/handler.py ~276-282); run_task/error.py _mark_terminal runs finalizers inside its retried closure. EARS: on_cleanup and stage finalizers shall be invoked after the completing transaction commits, outside the version-conflict retry: once per committed completion.
N6 CONFIRMED (probe_base_exception_task.py, both backends): a task raising SystemExit: the exception escaped QueueProcessor.process_one to the caller and the task stayed RUNNING (control RuntimeError: TERMINAL once). EARS: If a task raises a BaseException that is not an Exception (SystemExit, KeyboardInterrupt, GeneratorExit), then RunTask shall record the task TERMINAL with the exception's type and message, once; the processor shall not propagate it from the handler.
