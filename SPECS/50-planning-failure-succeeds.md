# 50 — every stage planning failure completed as SUCCEEDED

Ticket: #50 (high, bug). Found by #49's probe, not by the audit.

## EARS spec

- If planning a stage raises, then the stage shall complete with a failure
  status, not SUCCEEDED.
- When a stage records a planning failure, `determine_status` shall classify it
  through `failure_status()` so `continuePipelineOnFailure` and `failPipeline`
  are honoured.

## Technical problem

A failure that occurs before any task exists has no task status to carry it.

## Cause

`start_stage/handler.py` wrote `context["exception"]` and
`context["beforeStagePlanningFailed"] = True`, then pushed `CompleteStage`.
`models/stage/stage.py::determine_status`, for a RUNNING stage with no tasks,
returned SUCCEEDED and never consulted either key.

    grep -rn beforeStagePlanningFailed src/ tests/  ->  one hit: the write

Written at one site, read by none — the same shape as `_activated_branches`,
`_completed_branches` and `_loop_scope`.

## Blast radius

Every planning failure, not one: a raising builder, an unknown reducer name
(`reducers.py` raises `ValueError`), ancestor-output failures, before-stage
construction failures. In each case the workflow reported SUCCEEDED having run
nothing, with the real error in stage context where nothing looked.

This also refutes an earlier audit note claiming an unknown reducer name "kills
the workflow at runtime". It did not; it silently succeeded.

## Alternatives

- **`determine_status` consults the marker and returns `failure_status()` —
  CHOSEN.** One branch, reuses the existing classifier, and cannot affect a
  stage that has tasks because that takes a different branch.
- The handler sets the status directly — REJECTED: `determine_status` is the
  single place status is derived; a second writer is a second description of the
  same fact.
- Delete the unread flag — REJECTED: the flag was correct, the consumer was
  missing.

## Verification

    before:  WORKFLOW: SUCCEEDED   stage sw: SUCCEEDED tasks=0
    after:   WORKFLOW: TERMINAL    stage sw: TERMINAL  tasks=0

## Consumer impact

**This changes observable behaviour.** A pipeline whose planning has been failing
silently has been reporting success; it will now report failure. That is the
point of the fix, and it means an upgrade can turn a green pipeline red. The
error was always there — only the reporting changes.
