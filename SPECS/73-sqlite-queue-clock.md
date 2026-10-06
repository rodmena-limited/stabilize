# 73 — SQLite queue clock is exact UTC

Ticket: #73 (0.32.0). Unattended audit session 2026-10-06 (#70).

## EARS spec

- While the host timezone is not UTC, the SQLite queue shall deliver, lock and delay messages exactly as in UTC, on every SQLite version.
- The SQLite queue shall not deliver a delayed message before its deliver_at (sub-second precision).
- When the processed-message retention sweep runs with max_age_hours=N, it shall delete only marks older than N hours.

## Synthesis

- Solution domain: SQLite date/time functions (`'utc'` modifier semantics differ across releases; `julianday()` parses ISO-8601 with offsets).
- Alternatives: Python-supplied UTC now + julianday() both sides [CHOSEN] vs datetime('now') without 'utc' [REJECTED: still second-truncated, string formats differ] vs epoch REAL columns [REJECTED: schema change for every database].

## Evidence

`probe_sqlite_queue_clock.py` in python:3.11-slim-bookworm (SQLite 3.40.1) with published 0.31.0: New York re-polled a locked message and ignored a 2 h delay; London did not deliver an immediate message; UTC control correct; retention deleted a fresh mark on any SQLite. Fixed source passes in all three zones. `tests/test_sqlite_queue_clock.py` red 3/3 on 0.31.0.
