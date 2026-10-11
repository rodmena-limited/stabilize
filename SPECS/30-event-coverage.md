# 30 — The event log must record what the engine did

Status: in-progress · Ticket: issuedb #30 · 2026-09-19

## EARS

- When a stage suspends awaiting a signal, the engine shall record a
  stage-suspended event.
- When a suspended stage is resumed by a signal, the engine shall record a
  stage-resumed event carrying the signal name.
- When a jump is applied, the engine shall record a jump-executed event naming
  the source stage, the target stage and the jump type.
- When a task is rescheduled after a transient failure, the engine shall record
  a task-retried event carrying the attempt number.
- Where a caller supplies an identity with a signal, the engine shall record
  that identity as the event actor; otherwise the actor shall remain `system`.
- The engine shall write each of these events inside the transaction that
  commits the state they describe.
- When replaying, the engine shall reconstruct a suspended stage as SUSPENDED.
- If a future change removes any of these call sites, the test suite shall fail.

## Problem

Seven recorder methods had zero call sites while `EventReplayer` carried live
branches for all of them — dead code that could never fire. There was no event
type for suspension at all. And every event was attributed to `"system"`:
`events/recorder/context.py` already accepted an actor, but the handler wrapper
passed only the correlation id, dropping the identity at the one point it could
have been supplied.

Operationally: a human-approval wait was an unexplained silence between
`task.started` and `task.completed` and replayed as RUNNING; a workflow that
looped forty times replayed as though it ran once; a retry storm was invisible;
and "who released the production gate" had no answer on any surface.

```
before: actors seen: ['system']    types: no suspension, no jump, no retry
after:  actors seen: ['alice', 'system']
```

## Resolution

`stage.suspended`, `stage.resumed`, `jump.executed` and `task.retried` are
recorded, each inside the transaction that commits the state it describes — via
the new `execute_atomic(during_txn=...)` hook, or the handler's own transaction.
`hitl.approve`/`reject`/`send_signal` accept `user=`, carried on the message
rather than merged into `signal_data`, and recorded as the actor.

### Decisions worth recording

- **`jump.executed` fires only at the real jump site.** The target-not-found and
  budget-exceeded paths also call `_apply_jump`, but no jump occurs there, so
  recording one would misreport.
- **`user` goes on the message, not in `signal_data`.** `ApprovalTask` exposes
  `signal_data` verbatim as `outputs["approval"]`, so merging would silently
  change existing programs' outputs — a behaviour change disguised as additive.
- **Events are recorded inside the state transaction.** The natural choke points
  for status changes run *before* the transaction in most handlers; recording
  there would append events that survive a rollback.

## Deliberately not wired yet

- `record_status_change`, `record_context_updated`, `record_outputs_updated` —
  their choke points precede the transaction, so they need a
  buffer-then-flush-inside approach rather than an eager append.
- `record_workflow_paused` / `record_workflow_resumed` at store level —
  `WorkflowStore` has no injected recorder and the pause helpers commit
  themselves.

## Verification

`audit/evaluations/probe_event_coverage.py`, registered in `run_all.sh`, with a
control asserting the previously-wired lifecycle events still fire.
`tests/events/test_event_coverage.py` — 7 tests, 2 of them controls (lifecycle
events keep firing; unattributed actions still record as `system`). Reverting the
four call sites fails all five finding tests while both controls stay green.
