# 92 — Red-proof every test and probe: each must be shown to fail on a mutant of the engine, enforced by a gate job

Ticket: #92. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

EARS SPEC:
- The stabilize test suite shall prove, for every collected test id (tests/ and golden_standard_tests/, sqlite and postgres parameters), that the test fails on at least one mutant of src/stabilize while it passes on the unmutated source.
- When a test fails on a mutant, the red-proof tool shall count it as proven only if the same test passes on the unmutated tree in the same run and fails on that mutant on 2 of 2 runs.
- If a mutant makes the package fail to import or the test collection fail, then the red-proof tool shall discard that mutant and shall not count any test as proven by it.
- If a test is not proven after every mutant on the source lines it executes has been tried (cap 12 mutants per line), then the red-proof tool shall list it as UNPROVEN with the number of mutants tried.
- The gate job redproof shall exit non-zero when any UNPROVEN test id is absent from audit/redproof/unproven_allowed.txt, and when any entry in that file lacks a written reason or names a test that no longer exists or is now proven.
- The red-proof tool shall write audit/redproof/proof.tsv (test id, mutant file:line, operator, failure kind) so any proof can be re-run by hand with one command it prints.
- Every probe in audit/evaluations shall be observed printing a [FAIL] line on 0.32.1 (expect lists) or on a recorded source mutant (audit/redproof/probe_mutants/), or be listed as vacuous with the mutations tried.
- The red-proof run shall complete within 3 h at 2 parallel workers on vm-1 under the gate load limits (start below load 8, MemAvailable >= 6 GB).

TECHNICAL PROBLEMS:
1. Mutation analysis inverted: per test, existence of a killing mutant (classic mutation testing reports per mutant).
2. Test selection per mutant from per-test line coverage, to keep runs bounded.
3. Distinguishing red caused by the mutation from flakiness or collection breakage.
4. A ratchet that cannot pass vacuously (allowlist entries re-validated each run).

SOLUTION DOMAINS:
- Mutation testing (DeMillo/Lipton/Sayward 1978; mutmut, cosmic-ray operator sets): relational, boolean, negation, constant, return-value, statement-deletion operators.
- coverage.py dynamic contexts (--cov-context=test) for line -> test mapping.
- Greedy set cover over (mutant -> failing tests) to minimise runs.

ALTERNATIVES:
- Tool: own AST mutator + coverage contexts + greedy cover [CHOSEN] vs mutmut 3 [REJECTED: reports per mutant, stops at the first failing test, so it cannot say which tests are able to go red; trampoline rewriting changes import paths the suite asserts on] vs cosmic-ray [REJECTED: same per-mutant model, needs a session DB and a config per module].
- Proof threshold: 2/2 fails on mutant + pass on baseline [CHOSEN] vs single failure [REJECTED: one flaky failure would count as proof].
- Scope: all collected tests [CHOSEN, operator directive] vs only the audit tests [REJECTED by the operator].
