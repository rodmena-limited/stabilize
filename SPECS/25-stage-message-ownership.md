# 25 — Stage-level messages must be scoped to their own workflow

Status: in-progress · Ticket: issuedb #25 · Audit finding C1 · 2026-09-19

## EARS specification

- When a `StageLevel` message is handled, the engine shall verify that the
  resolved stage belongs to the workflow named by `message.execution_id`, and
  shall refuse the message otherwise.
- Where a stage-level message names a stage belonging to a different workflow,
  the engine shall not mutate that stage, shall mark the message processed so it
  is not redelivered, shall emit an `InvalidStageId` marker, and shall log at
  WARNING naming both the claimed and the actual workflow.
- Where the resolved stage has no attached workflow, the engine shall treat the
  message as unroutable by the same path rather than raising.
- The engine shall continue to deliver every correctly addressed stage-level
  message unchanged.
- If a future change removes the ownership check, then the engine's own test
  suite shall fail.

## Problem

`StabilizeHandler.with_stage` (`handlers/base.py:163-199`) resolves
`message.stage_id` by primary key and calls `block(stage)` without comparing
`stage.execution.id` to `message.execution_id`. No handler performs that
comparison either. Affected messages: `SignalStage`, `CancelStage`, `SkipStage`,
`JumpToStage`, `RestartStage`, `ResumeStage`, `AddMultiInstance`.

## Reproduction

`audit/evaluations/probe_stage_message_ownership.py`, driving only the public
`stabilize.hitl.approve` API:

```
suspended: A=SUSPENDED B=SUSPENDED
after cross-workflow approve: A=SUCCEEDED outputs={'approved': True, 'approval': {'by': 'caller-in-B'}}
```

The probe carries a control — a correctly addressed approve must still release
its own gate — so it cannot pass by breaking signal delivery outright.

## Constraints on any fix

- Read the workflow through `stage.has_execution()`; `stage.execution`
  (`models/stage/stage.py:177-189`) raises `ValueError` when unattached, and
  `with_stage`'s existing `except ValueError` wraps only `retrieve_stage`.
- A silent refusal is not acceptable: `InvalidStageId` is handled by a no-op
  marker handler (`queue/processor/mixins.py:137`), so the refusal must log and
  record an event of its own.
- `hitl.send_signal` is fire-and-forget; validate eagerly at the call site.

## Verification

- The probe goes green after the fix and red against the pre-fix commit.
- Regression tests cover every `StageLevel` message type, in both directions:
  a foreign `stage_id` is refused, and a correctly addressed message still works.
