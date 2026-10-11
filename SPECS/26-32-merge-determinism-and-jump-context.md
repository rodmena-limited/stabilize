# 26 / 32 — Deterministic ancestor merge, and jump context precedence

Status: #26 resolved · #32 open · 2026-09-19

## #26 — The ancestor merge must be deterministic

### EARS

- When outputs are merged from ancestor stages, the engine shall produce the
  same merged result for the same stored state regardless of process, host or
  `PYTHONHASHSEED`.
- Where two topologically incomparable ancestors write the same output key with
  different values, the engine shall report that collision rather than resolving
  it silently.
- The engine shall keep the existing list-concatenation behaviour and shall keep
  merging a key written by only one ancestor.

### Problem

Both backends carried the algorithm verbatim, seeding a topological sort from a
`set`. Python randomises string hashing per process, so sibling order — and
therefore which ancestor won a last-write-wins collision — varied by process.

```
before:  4 from_b / 4 from_c    across 8 hash seeds
after:   8 from_c               deterministic
```

### Resolution

Extracted to `dag/merge.py` and used by both backends, so they cannot drift
apart again. Ordering ties break on `ref_id`.

**Stability is not correctness**, which is why the tie-break alone was not
enough: with a total order imposed, the winner becomes whichever `ref_id` sorts
last, so renaming two branches would silently change which value survives. The
merge therefore also detects collisions between ancestors with no path between
each other and reports them, naming both ancestors, both values, and stating
that the tie-break is a convention rather than a semantic.
`STABILIZE_MERGE_STRICT=1` raises instead.

### Verification

`tests/test_diamond_pattern.py::TestOutputMerging::test_merge_winner_is_stable_across_processes`
spawns subprocesses with differing `PYTHONHASHSEED`; an in-process loop cannot
detect this. It was added as `xfail(strict=True)` while the defect stood, and
flipping to XPASS forced the marker's removal when the fix landed.

## Jump context must survive re-entry hydration

Ticket #27 made a re-entered stage prefer its ancestor's value over its own stale
copy. Correct in general — but a jump's carried context is a deliberate write,
not a stale copy, and was being clobbered by it. A loop-back carrying `i=1` had
it reset to the outer scope's `i=0` on the next plan.

The jump now drops the keys it writes from `_hydrated_keys`, for the following
plan only; that plan re-records the set from its own ancestors, so ordinary
ancestor precedence resumes afterwards.

This was found by tracing nested loops, not by reading — two correct-looking
changes of my own were fighting each other.

## #32 — Nested loops still exit one iteration short

Measured, outer 3 × inner 2 (6 expected):

```
before #26 and the jump fix : inner ran 2, counter stuck at i=0
after  #26                  : inner ran 7, counter still stuck at i=0
after  the jump-context fix : inner ran 5, counter CORRECT -> [0, 1, 0, 1, 0]
```

Two of three outer passes are now right; the third runs the inner body once
instead of twice. **Not traced to a line and deliberately not guessed at** —
tracked as #32. Single-level loops (#31) are unaffected and verified.
