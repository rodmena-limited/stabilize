"""Audit probe (#17): declared secrets in stage context.

This is a CHARACTERISATION harness for an OPEN defect, not a regression test
for a fix. It records two things:

  FINDING   a context key declared secret via the documented `secrets` list is
            still persisted in cleartext. This is ticket #17 and is expected
            to reproduce until #17 is resolved.

  CONSTRAINT any fix must keep the RUNNING task able to read the real value.
            Redacting at the persistence boundary was implemented and measured
            and it does NOT satisfy this: handlers re-read stage state from the
            store, so the task receives the placeholder. That attempt was
            reverted; this probe is the reason.

The constraint is asserted and FAILS the probe if violated. The finding is
reported, not asserted, so this harness does not go red every run for a defect
that is known and open.

Run:  python audit/evaluations/probe_secret_redaction.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    ShellTask,
    SqliteQueue,
    SqliteWorkflowStore,
    StageExecution,
    TaskExecution,
    TaskRegistry,
    Workflow,
)

SECRET = "TOKEN_THAT_MUST_NOT_PERSIST"
# Compare inside the task and print only a verdict: echoing the value would put
# it into outputs, which are persisted too, and the probe would then be
# measuring its own leak.
COMMAND = f'if [ "{{token}}" = "{SECRET}" ]; then echo MATCH; else echo MISMATCH; fi'


def run(db: Path) -> tuple[str, list[str]]:
    """Run one workflow whose task reads a declared secret."""
    store = SqliteWorkflowStore(f"sqlite:///{db}", create_tables=True)
    queue = SqliteQueue(f"sqlite:///{db}", table_name="queue_messages")
    queue._create_table()
    registry = TaskRegistry()
    registry.register("shell", ShellTask)
    processor = QueueProcessor(queue, store=store, task_registry=registry)

    workflow = Workflow.create(
        application="audit-probe",
        name="declared secret",
        stages=[
            StageExecution(
                ref_id="1",
                type="shell",
                name="read the secret",
                context={"command": COMMAND, "token": SECRET, "secrets": ["token"]},
                tasks=[
                    TaskExecution.create(
                        name="sh", implementing_class="shell",
                        stage_start=True, stage_end=True,
                    )
                ],
            )
        ],
    )
    store.store(workflow)
    Orchestrator(queue).start(workflow)
    processor.process_all(timeout=30.0)
    verdict = str(store.retrieve(workflow.id).stages[0].outputs.get("stdout", "")).strip()
    store.close()
    queue.close()

    # Live rows only. Scanning the raw file finds freed SQLite pages, which is
    # data remanence rather than "the engine stored it" — a different claim
    # with a different fix (secure_delete/VACUUM), and one this probe does not
    # make.
    holders: list[str] = []
    connection = sqlite3.connect(db)
    for (table,) in connection.execute("select name from sqlite_master where type='table'"):
        for row in connection.execute(f"pragma table_info({table})"):
            column = row[1]
            try:
                count = connection.execute(
                    f"select count(*) from {table} where cast({column} as text) like ?",
                    (f"%{SECRET}%",),
                ).fetchone()[0]
            except sqlite3.Error:
                continue
            if count:
                holders.append(f"{table}.{column} ({count} row(s))")
    connection.close()
    return verdict, holders


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        verdict, holders = run(Path(tmp) / "probe.db")

        print("=" * 72)
        print("CONSTRAINT — a running task must receive the real value")
        print(f"  task verdict: {verdict!r}")
        if verdict == "MATCH":
            print("  >>> PASS: the task read the declared secret")
        elif verdict == "MISMATCH":
            print("  >>> FAIL: the task received a placeholder, not the value.")
            print("      Persist-time redaction was measured to cause exactly this")
            print("      and was reverted. Any fix must not reintroduce it.")
            return 1
        else:
            print("  >>> FAIL: the task did not run; this probe measured nothing")
            return 1

        print()
        print("=" * 72)
        print("FINDING (#17, OPEN) — is the declared secret in the database?")
        if holders:
            for holder in holders:
                print(f"  cleartext in: {holder}")
            print("  >>> REPRODUCES: ticket #17 is open and this is its evidence.")
            print("      Not a probe failure; it is the defect this probe characterises.")
        else:
            print("  >>> NOT REPRODUCED: no live row holds the secret.")
            print("      If #17 is still open, check that this probe can still see a")
            print("      secret at all before concluding it is fixed.")

    print()
    print("=" * 72)
    print("RESULT: CONSTRAINT HOLDS (see FINDING above for the open defect)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
