# 110 — A timer handled after its due time fires (0.34.5)

Ticket: issuedb #110. Reported by procurement-desk-95465d on AgentBus, thread
`01M7KNFGRX2DTJ8BE296AEGQD9`, against 0.34.4.

## What was wrong

The RunTask handler compares `now - task start` with the task's lifetime before it calls
`execute()` (`handlers/run_task/handler.py`). `TimerTask.get_dynamic_timeout` returned
`(due - armed) + 5 min` once armed and 1 h before. A timer handled later than that ended
TERMINAL with "Task exceeded total timeout" and never fired. `WaitTask` inherits it.

Reproduced on the 0.34.4 tree on 2026-11-17, SQLite, through the real processor:

| Case | 0.34.4 |
|---|---|
| Control: processor up through the due time | SUCCEEDED |
| Grace 1 s, handled 2.5 s after due (the report) | TERMINAL, `3510ms > 2000ms` |
| Shipped timer, due 1 h ago, armed 2 h ago | TERMINAL, `7200005ms > 3900000ms` |
| Shipped timer, first run 10 min after task start, due now | TERMINAL, `660005ms > 360000ms` |
| Shipped timer, never armed, task started 2 h ago | TERMINAL, `7200005ms > 3600000ms` |

The third case needs no outage. The lifetime runs from the task's start and the allowance
from arming, so a first run more than five minutes late fails the timer at its due time.

## EARS spec

- E1. When a `TimerTask` or `WaitTask` is handled at or after its due time, however long
  after, the engine shall run it and it shall fire.
- E2. When a `TimerTask` is first handled later than its task start by any interval, the
  engine shall arm it and fire it at its due time.
- E3. The engine shall not fail a `TimerTask` or `WaitTask` for exceeding a total lifetime;
  its lifetime shall be at least 36,500 days.
- E4. The limit on one `execute()` call of a `TimerTask` shall stay `(due - armed) + grace`
  once armed and `unarmed_timeout` before, as in 0.34.4.
- E5. If a `SubWorkflowTask` reaches its timeout and its child workflow has already
  completed, then the task shall take the child's result (SUCCEEDED, or TERMINAL naming the
  child's status) and shall not report a timeout.
- E6. If a `SubWorkflowTask` reaches its timeout and its child is still running, then the
  task shall cancel the child and end TERMINAL, as in 0.34.4.
- E7. If a `HighwayTask` reaches its timeout and holds a run id, then it shall poll Highway
  once; a completed, failed or cancelled run shall give its own result, and a run still in
  progress or unreachable shall end TERMINAL as in 0.34.4.
- E8. The task guide shall state that a timer has no lifetime, that a `RetryableTask`
  lifetime is wall-clock time that includes time the engine was not running, and that
  `on_timeout` is where a task looks once more.
- E9. The flow-control guide and the CLI prompt shall state that the jump limit is counted
  on the jumping stage and carried to its target, not once per execution; the task guide
  shall state that `scheduled_time` is recorded and the delay comes from
  `get_backoff_period`.

No quality threshold applies beyond E3 and E4. No schema, migration or public signature
changes.

## Technical problems

1. A deadline check that runs before the task is asked. For a timer the only correct
   outcome of a late delivery is to fire, so the check can only do harm.
2. The same check on built-in polling tasks whose answer arrived while the engine was down
   (`SubWorkflowTask` with `sub_workflow_timeout_seconds`, `HighwayTask`).
3. Documentation that disagrees with the engine: jump limit scope, `scheduled_time`,
   lifetime under downtime.

## Solution domains

- Durable timers: a timer is a due time, not a budget of work. BPMN timer events fire late
  after downtime; they do not fail.
- Deadline with a last look: the documented `on_timeout` hook, whose returned `TaskResult`
  is processed as the task's result.
- "No cap" lifetime: the existing pattern, `SubWorkflowTask.get_timeout()` returning
  `timedelta(days=36500)`.

## Alternatives

**Timer lifetime**

- Chosen: none. `get_timeout()` returns 36,500 days; `get_execution_timeout()` keeps the
  0.34.4 formula so one `execute()` call is bounded as before.
- Rejected: an `on_timeout` that fires. It would have to arm, re-arm and fire, which is
  `execute()`. Every late delivery would still log a timeout, and the per-call timeout path
  would fire a hung timer.
- Rejected: a larger grace. Any finite grace is an outage length that fails every due timer.

**Engine lifetime check**

- Chosen: unchanged.
- Rejected: run `execute()` once more before timing out any `RetryableTask`. It changes the
  documented contract for every task on about 50 platforms. A task whose first `execute()`
  submits work would submit after its deadline and then be abandoned.

**Built-in polling tasks**

- Chosen: a last look in `on_timeout`.
- Rejected: leave as documented. An engine outage longer than the timeout turns a finished
  child or run into a failed workflow.

## Call sites (review rule)

`get_dynamic_timeout` / `get_timeout` / `get_execution_timeout`:

- `handlers/run_task/handler.py:269` — the lifetime check. A timer never exceeds it now.
- `resilience/timeouts.py:43` — calls `get_execution_timeout` for a `RetryableTask`.
  `TimerTask` overrides it with the 0.34.4 formula, so the per-call limit is unchanged.
- `resilience/timeouts.py:45` — duck-typed fallback for a task that is not a
  `RetryableTask`. Not reached by `TimerTask`.
- `tasks/interface.py:178` — the default `get_execution_timeout`, overridden by `TimerTask`.
- `tasks/sub_workflow.py:70`, `tasks/highway/task.py:94` — their own overrides, unchanged.

`on_timeout` is called from `handlers/run_task/guards.py:38` (lifetime and per-call
timeouts) and `resilience/process_executor.py:99` (process isolation). `SubWorkflowTask`
refuses process isolation. `HighwayTask.on_timeout` makes one GET with the 30 s limit its
poll already has.

## Not built here

The procurement desk also asked for a wait that takes a deadline, escalates when it passes
and keeps waiting. That is a feature, not a defect, and is not in 0.34.5.

## Evidence

Tests: `tests/test_late_handling.py`. Red and green runs and the release digests are
recorded on the ticket.
