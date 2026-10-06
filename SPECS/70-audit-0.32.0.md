# 70 — Audit of 0.31.0 with two falsification passes; one release (0.32.0)

Ticket: #70. Unattended session 2026-10-06 (Farshid away, /ooo).

## EARS spec

- The audit shall run at least two falsification passes: pass 1 (claim inventory, live reproduction of every finding, red on 0.31.0 for the stated reason), pass 2 (an independent reviewer attacking the fixed diff, plus the CI job `falsify` that installs published 0.31.0 and requires every new probe to fail on its stated case).
- If a defect is CONFIRMED, then it shall be fixed in 0.32.0 with a regression test and an `audit/evaluations` probe.
- The release shall pass the full suite on SQLite and PostgreSQL before it is published.
- The published wheel shall be installed from PyPI into a clean environment and probed before the release is announced.

## Constraint discovered during the session

vm-1 is under a director-ordered cease on test suites, lint, type-checks and builds (this session's `pytest -n 16` caused a load-215 OOM). All verification after 08:20Z runs in ci.rodmena.co.uk (`.rodmena/ci.yml`); one `uv build` on vm-1 is approved by infra-manager when CI is green and the 1-minute load is below 8.

## Findings and tickets

#62 #63 #64 #65 #66 #67 #68 #69 (known-open, all CONFIRMED live this session where previously "by reading"), #71 SQLite DLQ table, #72 int4 queue ids, #73 SQLite queue clock, #74 PostgreSQL queue timestamps, #75 re-queue fork. Retracted: processed_messages.execution_id VARCHAR(26) (pipeline id is the same width, so an over-long id is refused at store()).
