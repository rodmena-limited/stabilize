# 26 — Ancestor-output merge must be deterministic across processes

Status: open · Ticket: issuedb #26 · Audit finding C5 · 2026-09-19

## EARS specification

- When outputs are merged from ancestor stages, the engine shall produce the
  same merged result for the same stored state regardless of process, host or
  `PYTHONHASHSEED`.
- Where two topologically incomparable ancestors write the same output key with
  different values, the engine shall report that collision rather than resolving
  it silently.
- The engine shall continue to merge a key written by only one ancestor, and
  shall keep the existing list-concatenation behaviour.
- If a future change reintroduces order dependence, then the engine's own test
  suite shall fail.

## Problem

`persistence/sqlite/queries.py:232-283` (and the PostgreSQL equivalent) builds
`ancestors` as a `set[str]`, builds the adjacency list by iterating it, and
seeds Kahn's algorithm from it. `str` hashing is randomized per process, so
sibling order — and therefore the winner of a last-write-wins key collision —
varies per process.

## Reproduction

Same diamond, same stored data, 8 `PYTHONHASHSEED` values, through the public
`get_merged_ancestor_outputs`:

```
4 from_b
4 from_c
```

## Why the existing guard missed it

`tests/test_diamond_pattern.py:511` exists to detect exactly this and calls
`pytest.fail` on non-determinism, but runs all iterations inside one
interpreter, where hashing is fixed for the process lifetime. It cannot fail on
the real defect. A cross-process guard now exists as
`TestOutputMerging::test_merge_winner_is_stable_across_processes`, marked
`xfail(strict=True)`.

## Constraint on any fix

Sorting the Kahn seed buys reproducibility, not correctness: with a total order
imposed, the winner becomes whichever `ref_id` sorts last, so renaming branches
silently changes the surviving value (measured: `approve_prod`/`abort_prod` →
PRODUCTION; `prod_approve`/`staging_abort` → STAGING, same DAG). No total order
over incomparable ancestors is more correct than another. Ship the sort together
with a collision detector, and document the tie-break as a convention.

`get_merged_ancestor_outputs` is a different function from
`get_upstream_stages`/`get_downstream_stages`; adding `ORDER BY` to those is not
this fix. `Workflow.get_context()` → `dag/topological.py:79-81` shares the
pattern.

## Related

`reducers.py` was built to work around fan-in ordering but writes the reduced
value to `stage.context` only, never promoting it to `outputs`; one hop
downstream the raw branch outputs are re-merged and this defect picks the
survivor (measured: join sees `['A','B','C']`, next stage sees `C`). Fix
together.
