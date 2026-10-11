# 27 / 28 — Re-entry hydration, and event read forward compatibility

Status: in-progress · Tickets: issuedb #27, #28 · 2026-09-19

Two prerequisites for the wider work: #27 unblocks structured loops (and fixes a
live defect in the documented WCP-10 pattern); #28 must ship as a 0.26.x patch
before any newer build writes an event type or bumps the event schema.

## #27 — A re-entered stage must observe fresh upstream outputs

### EARS

- When a stage is planned, the engine shall hydrate its context from merged
  ancestor outputs.
- Where a stage is planned again after a jump, restart or loop-back, the engine
  shall re-hydrate from the current ancestor outputs any key whose previous
  value came from an ancestor, rather than preserving the stale persisted copy.
- The engine shall continue to let a key set directly on the stage's own context
  take precedence over an ancestor output.
- Where an ancestor no longer publishes a key it published previously, the
  engine shall preserve the existing value rather than deleting it.
- The engine shall not change hydration for a stage planned only once.
- If a future change reintroduces the shadowing, the test suite shall fail.

### Problem

`handlers/start_stage/planner.py` computed `merged = ancestor_outputs`, let the
stage's own context override it, then assigned `stage.context = merged` and
persisted. The first value a stage observed for a key was frozen into its row
and shadowed every later ancestor output — a ratchet.

Reproduced through the documented `jump_to` retry pattern, no `LoopBuilder`:

```
upstream produced attempts = 11
checker observed each pass = [1,1,1,1,1,1,1,1,1,1,1]
workflow = TERMINAL          (exhausted the jump budget)
```

Users could not work around it: `run_task/handler.py:449` reloads the stage from
the store before `process_result`, discarding in-place context mutation.

### Resolution

Provenance. `_hydrated_keys` records the ancestor-sourced key set; on a later
plan a key in both that set and the current ancestor outputs yields to the
ancestor.

**Gating deliberately keys off the presence of `_hydrated_keys`, not off a loop
marker.** A stage planned once has no such key, so its first plan is unchanged;
only re-entry takes the new path. Gating on a `LoopBuilder`-specific marker was
considered and rejected — it would not have fixed the reproduction above, which
uses no builder.

After: `[1, 2, 3]`, three iterations, `SUCCEEDED`.

## #28 — The event read path must tolerate what a newer build wrote

### EARS

- When the engine reads an event whose `event_type` it does not recognise, it
  shall surface that event as an unknown type and shall continue returning every
  other event in the query.
- The engine shall preserve the original `event_type` string on an unrecognised
  event.
- When the engine reads an event whose `schema_version` is newer than its own,
  it shall refuse to apply it: raising under strict migration, skipping during
  replay.
- The engine shall continue to parse every recognised type and migrate
  same-version and older events exactly as before.
- If a future change removes the tolerant parse, the test suite shall fail.

### Problem

Five unguarded `EventType(...)` sites raised `ValueError` on an unrecognised
string — `events/base.py` (`Event.from_dict`), both store row converters, and
both subscription filter loaders. The exception escaped row→Event conversion, so
one unknown row failed the **entire** query: replay, `WorkflowStream` and every
durable subscription over that store.

Separately, `EventMigrator.migrate` walked only forward, so an event newer than
the reader fell through and was applied with the wrong field layout, silently.

### Resolution

`EventType.UNKNOWN` (valued `unknown.event`, following the existing
`category.action` convention so the pre-existing format test stays green) plus a
tolerant `parse_event_type()` at all five sites, preserving the original string
in `data["_raw_event_type"]`. The migrator refuses a future schema; replay skips
it. Replay's `if/elif` chains have no `else`, so `UNKNOWN` is already a no-op.

No behaviour change: no code path today can produce either condition.

## Verification

`audit/evaluations/probe_stage_context_rehydration.py`,
`audit/evaluations/probe_event_read_forward_compat.py` — both registered in
`run_all.sh`, both seen red before the fix and re-run red with the guards
reverted in memory, controls green throughout.
`tests/test_stage_context_rehydration.py` (4 tests, 3 of them controls),
`tests/events/test_read_forward_compat.py` (7 tests, 4 of them controls).
