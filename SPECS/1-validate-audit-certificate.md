# Spec: Validate AUDIT.md certificate on current HEAD; fix any regressions

**Ticket:** issuedb #1 · **Date:** 2026-07-30

## EARS requirements

- The stabilize repo shall reproduce the AUDIT.md certificate claims (section 7) on the current HEAD commit.
- When `make test-sqlite` and the golden-standard SQLite suite run on current HEAD, the stabilize test suite shall pass with 0 failures.
- When `tests/test_audit_certification.py` runs, all certification tests shall pass.
- When `tests/test_weak_spots.py` runs, all weak-spot coverage tests shall pass.
- When ruff and mypy run on the audited modules, they shall report no errors.
- If any audit-listed fix is found missing or regressed on current HEAD, then the stabilize repo shall be fixed so the corresponding regression test passes.
- If Docker is available, then the PostgreSQL test matrix and golden-standard suite shall also pass.
- The validation report shall state exactly which suites ran, against which backends, and name anything not exercised.
