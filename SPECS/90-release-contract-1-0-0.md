# 90 — 1.0.0 release contract - public API, stability policy, doc truth, Python floor, upgrade, sustained run (C19-C24)

Ticket: #90. Umbrella: #77 (audit 0.32.1 -> 1.0.0).

Status: release requirement for 1.0.0 (operator decision 2026-10-11). C24 public API freeze, C23 declared-but-false claims, C21 interpreter floor, C19 upgrade path, C22 sustained run.

## EARS spec

- The public API of 1.0.0 shall be exactly the names in stabilize.__all__ (127 at 0.32.1) plus the documented modules listed in docs/stability.rst; everything else is internal and may change in a minor release.
- docs/stability.rst shall state the semantic-versioning promise: no removal or incompatible change of a public name, message type, persisted schema or CLI command before 2.0.0; deprecations warn for at least one minor release.
- docs/stability.rst shall state the delivery guarantees as measured: at-least-once message handling, deduplicated by processed_messages; a task's side effects can repeat when a worker's lock renewal is down longer than lock_duration unless the task lease is enabled; hooks (on_cancel, on_cleanup, on_timeout) are at-least-once across a crash.
- No document in the repository shall claim a behaviour no test or probe exercises: CLAUDE.md's "issue 41: not yet wired up" (messages are validated at decode since #41) shall be corrected, and the README / docs swept.
- CI shall run the suite on Python 3.11 (declared floor) and 3.14, green.
- CI shall show work started under published 0.32.1 finishing under 1.0.0 after mg-up (upgrade_inflight.py).
- A sustained run of at least 30 minutes on PostgreSQL shall be reported with throughput, queue depth, processed_messages growth against retention, pool connections and RSS.

## Technical problems

1. Defining and documenting a stable surface for a library 50 platforms depend on.
2. Evidence for the release-level claims (floor, upgrade, sustained behaviour).

## Solution domains

- Semantic Versioning 2.0.0; the house dependency-bounds policy (floors only).

## Alternatives

- __all__ as the public surface [CHOSEN: already curated, 127 names] vs "everything importable" [REJECTED: freezes internals such as handler classes' private helpers] vs a new stabilize.api facade [REJECTED: breaks every existing import].

## Callers

- Documentation and CI only.
