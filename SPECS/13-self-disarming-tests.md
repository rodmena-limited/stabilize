# Spec: self-disarming tests (#13)

## EARS requirements

- The message-deduplication race test shall assert the desired contract
  unconditionally and be marked xfail(strict=True), so a future fix XPASSes and
  fails the suite, forcing the marker to be removed deliberately.
- The thread-local connection test shall fail, with the observed error, rather
  than converting a shortage of connections into a skip.
- No test shall report skipped for a reason that masks a failure or an
  unexercised half of the backend matrix.
- A test asserting a terminal state shall distinguish a TIMEOUT from a wrong
  terminal status.

## What was already in place

The dedup race test asserts `processing_count == 1` unconditionally under
`xfail(strict=True)`, and the thread-local test fails on a connection shortage.
Both verified present.

## What this pass found and changed

Two skips in tests/test_dlq_atomicity.py asserted a SQLite limitation that had
never been measured and does not exist:

    "SQLite doesn't handle high-concurrency DLQ operations reliably"
    "SQLite uses thread-local connections; concurrent DLQ tested via file-based DB"

The real cause was the fixture: the regular `queue` fixture is
`sqlite:///:memory:`, which is per-connection, so ten worker threads addressed
ten separate databases. Switched both tests to the existing `file_queue` /
`file_repository` fixtures. Both now run on SQLite and PASS — 50 messages with
10 concurrent move_to_dlq calls. Half the backend matrix had silently never run.

Remaining skips in that file (`backend == "postgres"` guarding SQLite-specific
mocking) are legitimate inverse guards and were left.

tests/test_workflow_control.py::test_cancel_stops_running_workflow polled with
a 15s wall-clock deadline and reported a wrong terminal status when the machine
was merely busy — observed once in a full-suite run and not reproducible in
isolation (8/8) or on a second full run. The budget is now 120s and a timeout
asserts as a timeout, naming the last status seen, so the test cannot report a
cancellation defect that did not occur.
