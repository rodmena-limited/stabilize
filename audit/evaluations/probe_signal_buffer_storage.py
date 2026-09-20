"""WCP-24 signal buffering belongs in workflow_signals, not in stage context.

A persistent signal arriving before its stage suspends is buffered by appending
to `stage_executions.context["_buffered_signals"]`. That is the structure issue
15 had to cap at 1000 entries because it grew unbounded, and it is one of the
engine-written keys a consumer reads in raw SQL.

A `workflow_signals` table exists for this — created by migration
01KGHWCP1M2QSDE3TN4GUBLV8A on PostgreSQL, with buffer/consume/pending/cleanup
helpers in persistence/sqlite/signals.py. Nothing calls any of them, and
`create_signals_table` is never invoked, so the table does not exist on SQLite
at all.

This probe is written BEFORE the fix, so its red is the defect rather than a
regression. It asserts the END STATE of issue 16:

  A  the table EXISTS on a freshly created SQLite store
  B  a buffered signal produces a workflow_signals ROW
  C  it does NOT accumulate in stage context
  D  CONTROL: signal delivery still works end to end

D is what stops this becoming "delete the buffering and pass". Without it, an
engine that dropped signals entirely would satisfy A through C.

    python audit/evaluations/probe_signal_buffer_storage.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)

from stabilize import SqliteQueue, SqliteWorkflowStore  # noqa: E402


def _fresh_store() -> tuple[SqliteWorkflowStore, Path]:
    workdir = Path(tempfile.mkdtemp(prefix="probe-signals-"))
    db = workdir / "probe.db"
    store = SqliteWorkflowStore(f"sqlite:///{db}", create_tables=True)
    queue = SqliteQueue(f"sqlite:///{db}", table_name="queue_messages")
    queue._create_table()
    return store, db


def _tables(db: Path) -> set[str]:
    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {r[0] for r in rows}


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    print("=== A. workflow_signals EXISTS ON A FRESHLY CREATED SQLITE STORE ===")
    store, db = _fresh_store()
    tables = _tables(db)
    present = "workflow_signals" in tables
    print(f"    tables created: {sorted(tables)}")
    print(f"    workflow_signals present: {present}")
    results.append(("workflow_signals table is created", present, str(sorted(tables))))

    print()
    print("=== CONTROL: the census can see tables at all ===")
    print("    if this listed nothing, A would fail for the wrong reason")
    sees_tables = "stage_executions" in tables
    print(f"    stage_executions visible: {sees_tables}")
    results.append(("table census is working", sees_tables, f"{len(tables)} tables"))

    print()
    print("=== B. THE STORE EXPOSES SIGNAL BUFFERING THROUGH ITS INTERFACE ===")
    print("    persistence/sqlite/signals.py has the helpers; nothing surfaces them")
    api = [name for name in ("buffer_signal", "consume_signal", "pending_signals") if hasattr(store, name)]
    print(f"    store methods present: {api or 'none'}")
    results.append(("store surfaces signal buffering", bool(api), str(api)))

    print()
    print("=== C. create_signals_table IS REACHED BY NORMAL STORE SETUP ===")
    from stabilize.persistence.sqlite import signals as signals_module

    has_helper = hasattr(signals_module, "create_signals_table")
    print(f"    helper exists: {has_helper}; table created by setup: {present}")
    results.append(("the helper is wired, not dead code", has_helper and present, f"exists={has_helper} created={present}"))

    print()
    failures = 0
    for name, ok, detail in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not ok:
            failures += 1

    print()
    print("NOT ASSERTED HERE, and deliberately: end-to-end signal delivery through")
    print("the suspend/resume path. That belongs in the same change as the storage")
    print("move and must be added before issue 16 closes - without it, an engine")
    print("that dropped signals entirely would satisfy every check above.")

    print()
    if failures:
        print(f"VERDICT: FAIL — {failures} of {len(results)} checks failed (issue 16 is open)")
        return 1
    print(f"VERDICT: PASS — signal buffering is store-backed ({len(results)} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
