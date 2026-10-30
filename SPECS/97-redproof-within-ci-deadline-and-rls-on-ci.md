# 97 — Red-proof finishes inside CI's pipeline deadline; the RLS claim runs on CI

Ticket: issuedb #97.

## EARS spec
- R1. The red-proof gate shall complete inside one CI pipeline's deadline (3 h from pipeline creation), with every job under the runner's 2048 MB cap.
- R2. Red-proof shall prove each test with a known killing mutant from audit/redproof/known_proofs.tsv first (replay) and search mutants only for tests without one.
- R3. Where a test has no proof after the search budget, the gate shall fail and name it, unless it is on the allowlist with a reason.
- R4. The red-proof results (proof.tsv, unproven.tsv) shall be kept as CI artifacts so known_proofs.tsv can be updated from them.
- R5. The multi-tenant RLS claim shall run on CI as a pytest test against the CI database (no Docker), covering every engine table including workflow_signals and the 0.33.2 tables, isolation both ways, the bypass control, and queue polling under the policy.
- R6. A CI job shall build the sdist and wheel and assert the wheel's packaged files (migrations, prompt text) and version.

## Alternatives
- Shard the red-proof job across parallel CI jobs by test file, each replaying known proofs first [CHOSEN].
- Ask for a longer deadline [REJECTED as the only fix: a deadline is CI's to set, and replay-first is cheaper anyway].
