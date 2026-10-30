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

## Finding during 0.33.3 (2026-10-30)
The original diagnosis, that red-proof's work does not fit a 3 h pipeline, was not shown. In pipeline 4366 all four shards ran 170 minutes and never started a test: audit/redproof/run.py (and manual_proofs.py) wait for /proc/loadavg < 8 and MemAvailable >= 6 GiB, a vm-1 admission rule. In a CI container those files describe the shared node, so the wait never ended and logged nothing. The shards burned the repository's container-minutes quota and held pipeline 4367 on quota:container-minutes from 16:43Z.
- R7. While red-proof or the manual proofs run on CI, they shall not wait on host load (REDPROOF_HOST_GATE=off), and while either waits anywhere it shall log the wait at least every 5 minutes.
- R8. A red-proof shard shall time out within 130 minutes.

