# #104 — 0.34.1: a missing workflow raises `stabilize.errors.WorkflowNotFoundError` from every store

Ticket: issuedb #104. Found by a consumer's report on 2026-10-31. Present since the first store module (2025-12-18, fb9dda2); not a 0.34.0 regression.

## EARS SPEC
- When a workflow is absent, the stores (PostgreSQL and SQLite) shall raise `stabilize.errors.WorkflowNotFoundError`, carrying `execution_id`.
- The engine shall export one `WorkflowNotFoundError` class: `stabilize.persistence.WorkflowNotFoundError` shall be `stabilize.errors.WorkflowNotFoundError`.
- If a caller writes `except stabilize.StabilizeError` around a store call, then a missing workflow shall be caught, on both backends.
- The class shall still build from a positional id (`WorkflowNotFoundError("abc")`), with the message `Execution not found: abc`, so existing callers and tests keep working.
- The 0.34.1 release shall be gated by the full pytest suite on both backends plus the golden tests and examples (director, 2026-10-31: no red-proof, no gate); the wheel shall be built from `git archive` of the release commit.

## TECHNICAL PROBLEMS
1. One exception identity for one condition across two import paths (`stabilize.persistence.store` and `stabilize.errors`).
2. Constructor and message compatibility for callers that pass a positional id.

## SOLUTION DOMAINS
- The codebase's own errors hierarchy: `stabilize.errors.workflow.WorkflowNotFoundError(WorkflowError(StabilizeError))`, which `docs/guide/error_handling.rst` already documents as the class the stores raise.

## ALTERNATIVES
- Re-export `stabilize.errors.WorkflowNotFoundError` from `stabilize.persistence.store.criteria`, with the errors class taking the id positionally [CHOSEN: one class, both paths, the docs become true].
- Make the store's class subclass `StabilizeError` [REJECTED: two classes keep one name; `isinstance` differs by import path].
- Catch and re-wrap in the stores [REJECTED: three stores, hidden behaviour].

## Synthesis notes
- The suspected second defect from the same report (two unrelated `StabilizeError` bases) is not one: `stabilize.errors.StabilizeError` subclasses the top-level `stabilize.StabilizeError` through `StabilizeBaseException` (`errors/base.py:19`). Pinned by `test_engine_errors_are_caught_by_the_top_level_base`.
- `str()` of the errors class appends ` (code=301)`; the message prefix `Execution not found: <id>` is kept so `match=` assertions hold.
- Tests: `tests/test_error_hierarchy.py` (4 functions, 5 cases), red on 0.34.0 before the change.

## Done (2026-10-31)
- Red on the 0.34.0 code: 3 failed (one-class identity, missing workflow caught on sqlite and on postgres), 2 passed. Green after the change: 5 passed. Every test touching the class: 126 passed. ruff and mypy clean.
- Gate per the director: full suite on both backends with the golden tests and the headless examples, `-n 3`: 2949 passed, 11 skipped (10 need published old runners, 1 SQLite demo), 0 failed, 7 min 34 s.
- Release 0.34.1 built from `git archive` of the release commit with `SOURCE_DATE_EPOCH`; digests recorded on the ticket after upload.
