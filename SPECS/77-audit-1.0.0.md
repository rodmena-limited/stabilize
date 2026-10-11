# 77 — Audit of 0.32.1 to a 1.0.0 stability bar; release 1.0.0

Ticket: #77. Input: ci-conductor-dd94c8 report #143 (thread 01M4ME7M81P310BRX51Y24HYN5) and v3 items #1-#12 (thread 01M4NQV92G8321SKGSN8N82A4C, evidence rodmena-limited/ci audit/evaluations/stabilize-v3 @ fd3e2e8). Operator decision 2026-10-11: the release ships as 1.0.0, tests and probes go red before green, full adversarial refutation audit.

## EARS spec

- The audit shall treat every claim (peer report #143 and v3 #1-#12, docs, SPECS, docstrings, CHANGELOG) as SUSPECTED until a probe reproduces it on published 0.32.1 in ci.rodmena.co.uk.
- When a claim is reproduced red on 0.32.1 for its stated reason, the audit shall mark it CONFIRMED, open a ticket with EARS spec + synthesis + callers + die-here/two-at-once table + class sweep, and sweep the codebase for its class.
- If a probe or regression test passes on 0.32.1, then it shall not be counted as evidence; the claim shall be re-diagnosed once and otherwise recorded as not reproduced.
- Every regression test and probe shall be committed before its fix and observed failing in CI (branch run, and falsify_tests.sh / falsify_previous.sh against published 0.32.1) before the fix commit.
- Each design shall be attacked by a fresh-context refuter before implementation, and the final diff shall be attacked again before release.
- Every guard (concurrency limit, retention, lease, retry bound) shall be tested in both directions: blocks when exceeded and releases when it should.
- The release shall be 1.0.0: public API inventoried and frozen with a written semver policy; upgrade from the 0.32.1 schema with in-flight work exercised; the suite run on Python 3.11 (declared floor) and 3.14; a sustained run of >= 30 min on PostgreSQL reported with throughput, queue depth, processed_messages growth, pool connections and RSS.
- The published 1.0.0 wheel shall be installed from PyPI in CI and pass the suite and the full probe harness before it is announced.
- The audit report shall rank findings by customer harm, separate CONFIRMED from SUSPECTED, list claims not reproduced, and close with what was exercised / not tested / unsure.


## Technical problems

1. Falsification of an execution engine's at-most-once / no-stranding / true-outcome guarantees under crash, concurrency, retention and multi-queue deployment.
2. Evidence discipline: red-before-green for tests and probes, with execution only in CI (vm-1 cease).
3. Release contract for 1.0.0 (API surface, upgrade path, interpreter floor).


## Solution domains

- mission-critical-audit skill (claim inventory, live reproduction, class sweeps, both-direction guards).
- Existing harness: audit/evaluations (run_all.sh, falsify_previous.sh, _backends.py), .rodmena/ci.yml.
- Semantic Versioning 2.0.0 for the 1.0.0 public API promise.


## Alternatives

- Scope: whole-engine audit with the peer report as input [CHOSEN] vs fixing the 13 reported items [REJECTED by the operator: a fix list cannot find the defect nobody reported].
- Release: one release 1.0.0 [CHOSEN] vs 0.33.0 then 1.0.0 [REJECTED: two estate-wide upgrades for one change set, release cadence memory].
- Evidence of red: commit order + falsify scripts against the published previous wheel [CHOSEN] vs red observed locally [REJECTED: vm-1 cease; and a local red is not reproducible by the reader].

## Evidence mechanics

- `audit/evaluations/expect/<version>.probes` — `probe|[FAIL] marker` lines; `falsify_previous.sh` (OLD_VERSION=...) requires each listed probe to exit non-zero on the published old release with that marker. An empty list fails the check.
- `audit/evaluations/expect/<version>.tests` — `nodeid|marker` lines; `falsify_tests.sh` requires each listed test to fail with pytest exit 1 (an assertion, not a collection error) and the marker in its output, against the published old wheel from site-packages.
- CI `baseline` job: the existing suite and probes against published 0.32.1, excluding the entries in the expect files, so old guarantees are re-established on the old release before the audit hunts new ones.
- CI `py311` job: the suite on the declared floor (3.11, managed CPython via uv).
- New tests go in new test files, so the baseline exclusion is by file.

## Claim inventory, findings, not-reproduced claims

Maintained in the ticket comments and below as the audit proceeds.
