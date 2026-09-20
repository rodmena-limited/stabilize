"""STABILIZE_TASK_LEASE must provide leasing or refuse to start.

The flag is the operator asking for single-execution across processes. Catching
an initialisation failure and continuing leaves them believing they have a
mutual-exclusion guarantee they do not have, with a WARNING as the only trace —
worse than never offering the feature, because they will have stopped defending
against double execution some other way.

Both directions in one run: leasing ACTIVE when it can be provided, and a
REFUSAL when it cannot. A probe that only asserts the refusal cannot tell a
fail-closed engine from one that never leases at all.

    python audit/evaluations/probe_task_lease_fails_closed.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)

from stabilize import SqliteQueue, SqliteWorkflowStore, TaskRegistry  # noqa: E402
from stabilize.handlers.run_task.handler import RunTaskHandler  # noqa: E402
from stabilize.persistence import task_lease as task_lease_module  # noqa: E402
from stabilize.persistence.task_lease import TaskLeaseManager  # noqa: E402


def _handler() -> RunTaskHandler:
    workdir = Path(tempfile.mkdtemp(prefix="probe-lease-"))
    dsn = f"sqlite:///{workdir / 'probe.db'}"
    store = SqliteWorkflowStore(dsn, create_tables=True)
    queue = SqliteQueue(dsn, table_name="queue_messages")
    queue._create_table()
    return RunTaskHandler(queue, store, TaskRegistry())


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    print("=== A. FLAG UNSET — leasing is off and nothing is claimed ===")
    import os

    os.environ.pop("STABILIZE_TASK_LEASE", None)
    os.environ.pop("STABILIZE_TASK_LEASE_TTL_SECONDS", None)
    handler = _handler()
    off = handler.task_lease is None
    print(f"    task_lease is None: {off}")
    results.append(("flag unset leaves leasing off", off, f"task_lease={handler.task_lease!r}"))

    print()
    print("=== B. FLAG SET AND LEASING AVAILABLE — it must actually be ON ===")
    print("    this is the direction a refusal-only probe cannot see")
    os.environ["STABILIZE_TASK_LEASE"] = "1"
    handler = _handler()
    active = isinstance(handler.task_lease, TaskLeaseManager)
    owner = getattr(handler.task_lease, "owner", None)
    print(f"    task_lease active: {active}  owner={owner}")
    results.append(("flag set yields a live lease manager", active, f"task_lease={handler.task_lease!r}"))

    print()
    print("=== C. FLAG SET AND INITIALISATION FAILS — must REFUSE, not continue ===")
    original = task_lease_module.TaskLeaseManager

    class Unavailable:
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("InsufficientPrivilege: permission denied for schema public")

    task_lease_module.TaskLeaseManager = Unavailable  # type: ignore[misc]
    try:
        handler = _handler()
    except Exception as exc:
        refused = type(exc).__name__ == "TaskLeaseUnavailableError"
        names_consequence = "double execution" in str(exc)
        print(f"    RAISED {type(exc).__name__}")
        print(f"    names the consequence: {names_consequence}")
        results.append(("initialisation failure refuses", refused, f"{type(exc).__name__}"))
        results.append(("refusal names the consequence", names_consequence, str(exc)[:120]))
    else:
        print(f"    CONSTRUCTED ANYWAY, task_lease={handler.task_lease!r}")
        print("    -> leasing is silently off while the operator believes it is on")
        results.append(("initialisation failure refuses", False, "constructed with leasing disabled"))
        results.append(("refusal names the consequence", False, "no refusal raised"))
    finally:
        task_lease_module.TaskLeaseManager = original  # type: ignore[misc]

    print()
    print("=== D. FLAG SET AND TTL UNPARSEABLE — the non-privilege trigger ===")
    print("    the old catch was bare `except Exception`, so a bad TTL disabled leasing too")
    os.environ["STABILIZE_TASK_LEASE_TTL_SECONDS"] = "not-a-number"
    try:
        handler = _handler()
    except Exception as exc:
        refused = type(exc).__name__ == "TaskLeaseUnavailableError"
        print(f"    RAISED {type(exc).__name__}")
        results.append(("unparseable TTL refuses", refused, type(exc).__name__))
    else:
        print(f"    CONSTRUCTED ANYWAY, task_lease={handler.task_lease!r}")
        results.append(("unparseable TTL refuses", False, "constructed with leasing disabled"))
    finally:
        os.environ.pop("STABILIZE_TASK_LEASE_TTL_SECONDS", None)
        os.environ.pop("STABILIZE_TASK_LEASE", None)

    print()
    failures = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1

    print()
    if failures:
        print(f"VERDICT: FAIL — {failures} of {len(results)} checks failed")
        return 1
    print(f"VERDICT: PASS — leasing is provided or refused, never silently absent ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
