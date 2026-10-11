# 33 — De-selected branches must not execute

Status: in-progress · Ticket: issuedb #33 · Audit findings C2/C11 · 2026-09-19

## EARS

- When a split de-selects a branch, the engine shall not execute any stage on
  that branch.
- When every incoming edge into a stage is dead, the engine shall prune that
  stage rather than execute it.
- Where a stage has at least one live incoming edge, the engine shall execute it
  exactly once, regardless of how many of its other edges were de-selected.
- Where a stage carries no prune marker, the engine shall behave exactly as it
  did before.
- If a future change stops pruning de-selected branches, the test suite shall
  fail.

## Problem

An OR-split marked non-activated children SKIPPED, and SKIPPED is in
`CONTINUABLE_STATUSES`, so those children's own children evaluated READY and
ran. A join whose every upstream had been de-selected also fired.

```
before:  two-deep split   -> ran ['dead2', 'live', 'live2', 'root']
         all-pruned join  -> ran ['live', 'orjoin', 'root']
after:   two-deep split   -> ran ['live', 'live2', 'root']
         all-pruned join  -> ran ['live', 'root']
         diamond control  -> ran ['join', 'left', 'root']
```

## Why the two obvious fixes are both wrong

Both were simulated before being rejected:

1. **Push `SkipStage` to the children of a skipped stage.** `skip_stage.py`
   skips any NOT_STARTED stage *without consulting its other upstreams*, so the
   skip walks through the merge point and prunes the join, the tail and the rest
   of the DAG — then reports SUCCEEDED. Strictly worse than the defect: the bug
   runs too much loudly, this runs too little silently.
2. **Make readiness reject a skipped ancestor.** Converts every re-converging
   split into a `max_stage_wait_retries` stall (240 × 15s = 1 hour) followed by
   a false TERMINAL.

## Resolution

The decision is made at the **child's** readiness evaluation, the only place
where all of that child's upstreams are visible. `PredicatePhase.PRUNE` fires
when every incoming edge is dead. An edge is dead when the upstream split
recorded this child as non-activated (`_pruned_from`), or when the upstream was
itself pruned (`_pruned` **and** SKIPPED — the conjunction matters, because a
stage carrying the marker that ran anyway was reached by another live edge).

Pruned stages become SKIPPED: no new status, no persisted-shape change.
**`skip_stage.py` needs no change at all.**

`_record_activated_branches` is deleted. It iterated a partial workflow that
never contained the downstream join, so it wrote nothing, and it wrote outside
its caller's transaction.

## Compatibility

Absence of a marker means live, so an in-flight workflow upgraded mid-run is
unaffected: a split that already ran leaves its de-selected children SKIPPED
with no marker, and that subtree continues as before — no retroactive prune, no
stall.

**Behaviour change:** a join with no live branch is SKIPPED rather than
executed, and a de-selected subtree no longer runs.

**Scope boundary:** `stageEnabled=False` does *not* prune. A disabled stage in a
linear pipeline still lets its successor run — Orca-compatible, and possibly
relied upon. Gate semantics remain available via `split_type=OR` +
`split_conditions`. Making prune-on-skip the default later is a one-line change.

## Verification

`audit/evaluations/probe_branch_pruning.py`, registered in `run_all.sh`.
`tests/test_branch_pruning.py` — 5 tests × 2 backends, three of them controls
(the taken branch runs fully; a join with one live edge runs exactly once; a
linear workflow is untouched). Disabling the PRUNE phase fails both finding
tests while all three controls stay green.

Full suite 1582 passed / 5 skipped / 1 xfailed on both backends plus golden
tests — identical to the pre-change baseline.

## Still open

MULTI_MERGE (WCP-8) fires once rather than once per upstream completion. Needs a
re-arm-driven-from-completion design; deliberately not in this change.
