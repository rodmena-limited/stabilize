# 80 — A StartTask skip completes its stage; the skip is decided once (C8, C13)

Ticket: #80. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: CONFIRMED on published 0.32.1 (pipeline 2930). C8: stages [on, off] and [off, on] with off disabled (SkippableTask.is_enabled False) stayed RUNNING forever, also after two recovery sweeps; TaskResult.skipped() control SUCCEEDED. C13: is_enabled evaluated 4 times for 3 polls of one task; a task whose implementing_class is a dotted path was not checked at StartTask (started, then skipped at RunTask). Probes: probe_start_task_skip.py, probe_skip_decided_once.py. Peer v3 #5, #9 (their F19, P20).

## EARS spec

- When StartTask skips a disabled SkippableTask, the stage shall proceed exactly as if the task had returned SKIPPED: the next task starts, or the stage completes.
- StartTask shall resolve the task implementation the way RunTask does (get_by_class, then get), so the skip decision is taken at StartTask for every registered name form.
- SkippableTask.execute shall not re-evaluate is_enabled; is_enabled shall be called once per task start.
- A recovery sweep shall complete a RUNNING stage left by the 0.32.x wedge (covered by the lost-CompleteStage fix, #84).

## Technical problems

1. A completion message that the receiving handler's idempotency guard rejects (task already SKIPPED, guard expects RUNNING).
2. Two different task-resolution rules for the same task in two handlers.

## Solution domains

- Idempotent receiver keyed on state: the guard must accept the one non-RUNNING state its producer creates (SKIPPED with no end_time) and nothing else.
- Codebase: handlers/run_task/execution.resolve_task.

## Alternatives

- C8: CompleteTask accepts message.status SKIPPED for a task already SKIPPED with end_time None [CHOSEN] vs StartTask leaves the task RUNNING and lets CompleteTask set SKIPPED [REJECTED: the task would be observable as RUNNING and recovery would push RunTask for it] vs StartTask pushes StartTask/CompleteStage itself [REJECTED: duplicates CompleteTask's next-task logic, including the C0 halt rule].
- C13: one evaluation at StartTask [CHOSEN] vs evaluate on every execution (current) [REJECTED: a predicate that changes mid-run skips a task whose side effect already started] vs evaluate only at RunTask [REJECTED: costs a RunTask per disabled task and starts it].

## Callers (contract change)

- SkippableTask.execute no longer calls is_enabled: callers of execute are RunTask (via execute_with_timeout) and user code calling execute directly; the latter loses the check (changelog).
- CompleteTask idempotency guard: producers of CompleteTask(SKIPPED): StartTask skip (task SKIPPED, end_time None -> now accepted), RunTask manual skip (task RUNNING -> unchanged), _handle_success_like with SKIPPED result (task RUNNING -> unchanged). A redelivered CompleteTask(SKIPPED) after acceptance: end_time is now set -> ignored.

## Die-here / two-at-once

- Die after StartTask's skip transaction and before CompleteTask: task SKIPPED/no end_time; the queued CompleteTask completes it; if that message is lost, the stage is RUNNING with all tasks complete -> #84 recovery pushes CompleteStage (for [on, off]); for [off, on] recovery already pushes StartTask for the NOT_STARTED task.
