# 93 — File-size cap: 23 pre-existing files over 500 lines (15 over 550)

Ticket: #93. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

EARS SPEC:
- The stabilize repository shall contain no code file (src, tests, golden_standard_tests, audit, examples) longer than 550 lines, and no src file longer than 500 lines.
- When a test file is split, the red-proof proof records, manual proofs, allowlist and expect lists that name its test ids shall be updated in the same change, and the redproof gate job shall pass on that commit.

TECHNICAL PROBLEMS:
1. Fifteen files exceed the 550-line hard cap and eight more the 500-line soft cap, all already at that size in v0.32.1 (largest: cli/prompt_text.py 2525, tests/test_diamond_pattern.py 2108, tests/test_critical_corner_cases.py 1998, tests/test_workflow_control_flow_patterns.py 1840).
2. Test ids are referenced by audit/redproof and audit/evaluations/expect, so a split renames them.

SOLUTION DOMAINS:
- Module split by concern (codebase pattern: handlers/<name>/ packages).
- prompt_text.py is prose data; move it to a package data file read at runtime.

ALTERNATIVES:
- Split after the 0.33.0 release [CHOSEN, recommended to the operator]: pre-existing, no behaviour change, and a split forces a full red-proof rerun. Versus splitting inside 0.33.0 [REJECTED unless the operator asks: renames every test id the release evidence names].
