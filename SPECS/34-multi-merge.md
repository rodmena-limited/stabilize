# 34 — MULTI_MERGE (WCP-8) fires once per upstream completion

Status: in-progress · Ticket: issuedb #34 · Audit finding C11 · 2026-09-19

## EARS

- When an upstream of a MULTI_MERGE stage completes, the engine shall execute
  that stage once for that completion.
- The engine shall execute a MULTI_MERGE stage at most once per live upstream,
  and shall never fire twice for the same upstream.
- Where an upstream branch was pruned, the engine shall not fire for it.
- The engine shall record which upstream triggered each firing, readable as
  `context["_mm_trigger"]`.
- The engine shall preserve superseded firings in `context["_mm_firings"]`.
- Every other join type shall be unaffected.
- If a future change stops MULTI_MERGE firing per upstream, the test suite shall
  fail.

## Root cause

**My first hypothesis was wrong on its load-bearing claim**, and a refutation
pass caught it. I asserted that `evaluate_readiness` "names a caller that does
not exist" and that per-token semantics could not live at the readiness layer.

The caller exists: `start_stage/handler.py:503-511` is a post-claim,
per-join-type firing-bookkeeping hook with branches for DISCRIMINATOR and
N_OF_M — both of which are *also* predicates-plus-caller-state. It has had no
MULTI_MERGE branch since `6dbc21a` created all three evaluators in one drop.
**MULTI_MERGE is the unfinished third member of a family**, not a category error.

The engine has exactly three sanctioned ways for a stage to run more than once:

1. **Fresh identity** — a new `ref_id` (`MultiInstanceBuilder` at build time,
   `AddMultiInstance` at run time). The invariant is fresh identity, not build
   time.
2. **Explicit external re-arm of the one row** — `JumpToStage`, `RestartStage`,
   `LoopBuilder`, via `reset_stage_for_retry`.
3. **Never** — the default, enforced redundantly at four independent gates:
   the stage status guard, the claim CAS (`expected_phase="NOT_STARTED"`), the
   `StartTask` guard, and the `RunTask` execution-complete guard. Patching only
   the first changes nothing — measured.

WCP-8 is the only pattern requiring unsolicited, engine-internal multiplicity of
a single identity, demanded by graph shape with no message asking for it, and
neither mechanism 1 nor 2 can be triggered by graph shape.

## Why the obvious fix fails

The second token typically arrives while the stage is still RUNNING — the probe
log says `already RUNNING`, not `already SUCCEEDED`. Resetting a live execution
tears its state, and `workflow_control.py:191` (`RestartStage`) explicitly
refuses to re-arm a non-terminal stage, which is the engine's own considered
position on exactly this.

A naive trigger-time re-arm was measured in three regimes; one is worse than the
bug: the stage ends **CANCELED with empty outputs while the workflow records
SUCCEEDED** — silent record corruption.

## Resolution

Re-arm at **completion**, never at trigger time: the stage is terminal then, so
it respects `RestartStage`'s rule, and a `StartStage` arriving while the stage is
busy can be dropped because completion is guaranteed to re-drive.

`StartStage` gained a trailing defaulted `triggering_upstream_ref_id`, verified
wire-compatible in both directions. The predicate became stateful via
`_mm_consumed`, the exact analogue of the `_join_fired` its two siblings use.

## Documented limits

Stated in the guide, the enum docstring and `prompt_text.py` — the last being
what an LLM agent is handed as ground truth:

- firings are **serialised**, not concurrent
- `stage.outputs` holds the **last** firing; history in `_mm_firings`
- **multiplicity does not propagate** — measured: the merge's own downstream runs
  once, because the child hits the same status guard. Textbook WCP-8 passes a
  thread of control onward, needing one row per token per downstream stage. The
  documented contract is narrower and is what was implemented.

## Why it was never noticed

The only two tests call `evaluate_readiness` directly and assert
`READY`/`NOT_READY`. They never run a workflow, so they pass while the feature
does not work — the same vacuity pattern as the rest of this audit. No example,
no golden test, no submit-time validation, and unreachable from the config front
door since `launcher.py` never reads `join_type`.

## Verification

`audit/evaluations/probe_multi_merge.py`, registered in `run_all.sh`.
`tests/test_multi_merge.py` — 7 tests × 2 backends, **three of them controls**
(an AND-join over the same graph still fires once; DISCRIMINATOR unaffected; the
downstream runs once, pinning the documented limit). Reverting the re-arm fails
all four findings while every control stays green.
