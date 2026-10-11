# 31 — Structured loops (WCP-21) must execute

Status: in-progress · Ticket: issuedb #31 · Blocked-by: #26 (nesting only) · 2026-09-19

## EARS

- When a workflow built by `LoopBuilder.while_loop` runs, the engine shall
  execute the body once per iteration while the condition holds, and shall
  continue past the loop when it does not.
- When a workflow built by `LoopBuilder.repeat_until` runs, the engine shall
  execute the body at least once and shall continue past the loop once the
  condition is satisfied.
- Where a loop reaches `max_iterations`, the engine shall exit the loop, carry
  `loop_exhausted` downstream, record the failure in the terminating stage's
  status, and shall not fail the workflow.
- Where a loop condition references an identifier nothing publishes, the engine
  shall fail the stage naming that identifier rather than looping silently to
  the bound.
- The engine shall register the task classes its own stage builders emit, and a
  caller registering the same name shall still take precedence.
- If a future change breaks structured loops, the test suite shall fail.

## Problem

`loop_builder.py` emitted stages naming `LoopConditionTask` and `LoopBackTask`.
Neither class existed. `TaskRegistry.get_by_class` is a pure name lookup and
nothing seeded built-ins, so every loop died at its first condition check with
`Task type not found` and the workflow went TERMINAL. `LoopBuilder` is exported
public API, documented in the guide, and emitted verbatim by the LLM-facing
reference. Zero tests referenced it. `WaitStageBuilder` had the same defect.

## Resolution

New `tasks/loop.py` (`LoopConditionTask`, `LoopEntryTask`, `LoopBackTask`) and
`tasks/builtins.py`, seeded from `TaskRegistry.__init__`.

Measured, single level:

```
while_loop("i < 3")     -> body ran with i = [0, 1, 2]   SUCCEEDED
while_loop("i < 0")     -> body ran 0 times              SUCCEEDED
while_loop(max_iter=4)  -> 4 runs, FAILED_CONTINUE       SUCCEEDED
repeat_until("i >= 2")  -> body ran with i = [0, 1]      SUCCEEDED
```

### Decisions worth recording

- **Seeded from `TaskRegistry.__init__`, not the global default registry.**
  `QueueProcessor` takes an injected registry and callers construct a bare
  `TaskRegistry()`, so seeding the global reaches nobody. User registrations
  still override; the overwrite warning is suppressed only for built-ins.
- **Loops bound themselves.** `_loop_iteration` against `_loop_max_iterations`,
  resetting `_jump_count` on every jump so the generic 10-jump governor does not
  apply — exhausting that governor marks the stage TERMINAL and fails the
  workflow, which is not how a loop should end. Needs no change to
  `handlers/jump_to_stage/`, so the existing TERMINAL-at-budget tests stay green.
- **Loop state travels both ways.** The condition (or, for repeat-until, the
  entry) publishes the loop's variables as **outputs** so the body hydrates from
  them; the loop-back carries them in its jump **context**. Neither half alone
  works, because the condition sits upstream of the body and cannot otherwise
  observe what the body produced. Both halves were found by measurement, not by
  reading — the first implementation carried state back on the jump only, and
  the body still saw the initial value on every pass.
- **Exhaustion returns `FAILED_CONTINUE`.** A `while` that never satisfies its
  guard is a defect; this surfaces it in the stage status and in
  `loop_exhausted` without halting, so downstream compensation can run.

## Nesting (#32, resolved)

Nested loops work. Two things were required, both found by measurement:

1. **#26**, the deterministic ancestor merge — an inner loop-back no longer
   reads the enclosing loop's value at random.
2. **The condition publishes the iteration counter.** `LoopBackTask` read
   `_loop_iteration` from its own context, which on re-entry from an enclosing
   loop is left over from the previous pass, so it kept counting up from there.
   With a tight inner bound it reached the limit early and the last outer pass
   ran short.

```
outer 3 x inner 2   before: [0,1,0,1,0]     after: [0,1,0,1,0,1]
outer 2 x inner 3 -> 6      outer 4 x inner 2 -> 8
outer 3 x inner 1 -> 3      outer 1 x inner 4 -> 4
```

**Testing note.** The first nested test used `max_iterations=20` and passed with
the fix reverted: the accumulation only trips a *tight* bound, so a generous one
hides the defect. It was a vacuous test of exactly the kind this audit exists to
find. The bound is now `inner_n + 1`, and reverting the fix fails two of three
parametrised cases; the `(1, 4)` case passes by construction — one outer pass has
no accumulation to observe — and so acts as a control.

## Verification

`tests/test_structured_loops.py` — 7 tests × 2 backends, one a control asserting
a caller can still override a built-in name. Disabling the seeding fails five of
them with the original `Task type not found` symptom while the control stays
green.
