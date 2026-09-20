# 49 — an unregistered stage type silently resolves to a no-op

Ticket: #49 (high, bug).

## EARS spec

- If a stage has no tasks and no `StageDefinitionBuilder` is registered for its
  type, then the engine shall report it rather than complete it silently.
- Where `STABILIZE_STRICT_STAGE_TYPES` is set, the engine shall refuse such a
  stage instead of warning.
- Where a stage declares itself a coordinator (`mi_config`), the engine shall
  not report it.

## Technical problem

Distinguishing "a builder was written and never registered" from "a stage that
deliberately does no work". They are structurally identical.

## Why refusal is not available

`stage.type` is a free-form label, not a registry key. Only `noop` and `wait`
are registered, while this repo alone uses `test` (118x), `python` (68x),
`stage` (42x) and `shell` (37x). A multi-instance **parent** is also
legitimately taskless — its instances depend on it completing.

So `get()` raising on an unregistered type would break essentially every
workflow in existence. That was the original plan and it was wrong.

## Alternatives

- **Warn by default, raise under an env flag — CHOSEN.** Extracted from
  `STABILIZE_MERGE_STRICT` (`dag/merge.py`), the existing house pattern for
  exactly this situation. Cures the silence, breaks nobody, and gives callers
  who have no coordinator stages a way to get the hard guarantee.
- Raise in `get()` — REJECTED: breaks every unregistered-label workflow.
- Refuse on "no tasks + unregistered" — REJECTED: false-positives on
  multi-instance parents.
- Submit-time rejection — REJECTED: same false positive, and the information is
  only complete at plan time.

## Verification

`audit/evaluations/probe_unknown_stage_type.py`, 10 checks including **three
false-positive controls** — a registered builder, explicit tasks, and a declared
coordinator must all stay silent. Without those, a guard that warned on
everything would pass.

## Note

The `before/after/on-failure` half of this defect is **UNVERIFIED**. A probe case
for it had its own control fail, so its negative would have proved nothing; it
was removed rather than shipped green.
