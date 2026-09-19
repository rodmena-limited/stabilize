"""Audit probe: when shared circuit-breaker storage cannot be created, does
stabilize say so, and can an operator make it fail closed?

A postgresql:// URL is an explicit request for circuit state SHARED across
instances. Substituting process-local state means a breaker that should be
open everywhere stays closed on every other worker.

Run:  python audit/evaluations/probe_circuit_storage_honesty.py
"""

from __future__ import annotations

import io
import logging
import os
import sys


def _attempt(strict: bool) -> tuple[str, str]:
    """Return (outcome, captured log text) for one construction attempt."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    root = logging.getLogger()
    root.addHandler(handler)
    previous_level = root.level
    root.setLevel(logging.DEBUG)

    saved = os.environ.get("STABILIZE_CIRCUIT_STORAGE_STRICT")
    if strict:
        os.environ["STABILIZE_CIRCUIT_STORAGE_STRICT"] = "1"
    else:
        os.environ.pop("STABILIZE_CIRCUIT_STORAGE_STRICT", None)

    import resilient_circuit.storage as rcs

    from stabilize.resilience import circuits

    original = rcs.PostgresStorage

    class Unavailable:
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("SchemaNotReady: breaker table missing or drifted")

    rcs.PostgresStorage = Unavailable  # type: ignore[misc,assignment]
    try:
        storage = circuits._create_storage("postgresql://u:p@db.example/app")
        outcome = type(storage).__name__
    except circuits.CircuitStorageUnavailableError as exc:
        outcome = f"RAISED CircuitStorageUnavailableError: {exc}"
    finally:
        rcs.PostgresStorage = original  # type: ignore[misc]
        root.removeHandler(handler)
        root.setLevel(previous_level)
        if saved is None:
            os.environ.pop("STABILIZE_CIRCUIT_STORAGE_STRICT", None)
        else:
            os.environ["STABILIZE_CIRCUIT_STORAGE_STRICT"] = saved

    return outcome, stream.getvalue()


def _dsn_classification() -> list[tuple[str, str, bool]]:
    """Which DSN forms reach the PostgreSQL branch, observed by spying on it."""
    import resilient_circuit.storage as rcs

    from stabilize.resilience import circuits

    attempted: list[object] = []

    class Spy:
        def __init__(self, *args: object, **kwargs: object) -> None:
            attempted.append(kwargs.get("connection_string"))

    original = rcs.PostgresStorage
    rcs.PostgresStorage = Spy  # type: ignore[misc,assignment]
    results = []
    try:
        for url, why, expected in _DSN_CASES:
            attempted.clear()
            circuits._create_storage(url)
            results.append((url or "<None>", why, bool(attempted) == expected))
    finally:
        rcs.PostgresStorage = original  # type: ignore[misc]
    return results


_DSN_CASES: list[tuple[str | None, str, bool]] = [
    ("postgresql://u:p@h/db", "canonical", True),
    ("postgresql+psycopg://u:p@h/db", "sqlalchemy style", True),
    ("postgres://u:p@h/db", "postgres:// -- emitted by build_db_url", True),
    ("POSTGRESQL://u:p@h/db", "upper-case scheme", True),
    ("  postgresql://u:p@h/db  ", "surrounding whitespace", True),
    ("host=h dbname=d user=u", "libpq keyword/value", True),
    ("sqlite:///x", "sqlite must NOT reach PG", False),
    (None, "no DSN must NOT reach PG", False),
    ("", "empty DSN must NOT reach PG", False),
]


def main() -> int:
    failures = 0

    print("=" * 72)
    print("DSN CLASSIFICATION — a form misread as 'no database' silently selects")
    print("process-local circuit state. Both directions, so it cannot pass by")
    print("simply routing everything to PostgreSQL.")
    for url, why, ok in _dsn_classification():
        print(f"  {'ok  ' if ok else 'FAIL'} {url!r:34} {why}")
        if not ok:
            failures += 1
    print()

    print("=" * 72)
    print("CONTROL: a working backend must be reported, and reported ONLY on success")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
    from stabilize.resilience import circuits

    sqlite_storage = circuits._create_storage("sqlite:///x")
    logging.getLogger().removeHandler(handler)
    print(f"  sqlite url -> {type(sqlite_storage).__name__} (expected InMemoryStorage)")
    if type(sqlite_storage).__name__ != "InMemoryStorage":
        print("  >>> CONTROL FAILED: probe cannot observe backend selection")
        return 1
    print("  >>> control green")

    print()
    print("DIRECTION 1 — default: degrade, but say so at ERROR")
    outcome, logs = _attempt(strict=False)
    print(f"  outcome            : {outcome}")
    error_lines = [ln for ln in logs.splitlines() if "ERROR" in ln or "unavailable" in ln.lower()]
    print(f"  ERROR-level lines  : {len(error_lines)}")
    for line in error_lines:
        print(f"    {line[:150]}")
    claims_success = "Using PostgreSQL storage for circuit breakers" in logs
    print(f"  falsely claims PG  : {claims_success}")
    if not error_lines:
        print("  >>> FAIL: degradation is silent (no ERROR)")
        failures += 1
    elif claims_success:
        print("  >>> FAIL: logs claim PostgreSQL storage is in use when it is not")
        failures += 1
    elif "PROCESS-LOCAL" not in logs:
        print("  >>> FAIL: the ERROR does not name the consequence")
        failures += 1
    else:
        print("  >>> PASS: loud, and names the consequence")

    print()
    print("DIRECTION 2 — STABILIZE_CIRCUIT_STORAGE_STRICT=1: must fail closed")
    outcome, _ = _attempt(strict=True)
    print(f"  outcome            : {outcome[:120]}")
    if outcome.startswith("RAISED"):
        print("  >>> PASS: startup aborts rather than degrading")
    else:
        print("  >>> FAIL: strict mode still degraded silently")
        failures += 1

    print()
    print("=" * 72)
    print(f"RESULT: {'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
