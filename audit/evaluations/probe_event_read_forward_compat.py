"""Audit probe (#28): an old reader must tolerate events a newer writer produced.

Two hazards on the event read path, both of which make a forward-compatible
reader a hard prerequisite for ever adding an EventType or bumping the event
schema:

  FINDING A  an unrecognised event_type raises ValueError inside row->Event
             conversion, which takes out the ENTIRE query -- replay, every
             durable subscription and WorkflowStream -- not just that row.

  FINDING B  an event whose schema_version is NEWER than this reader's falls
             through EventMigrator.migrate unchanged (the chain only walks
             forward) and is then applied with the wrong field layout, silently.

  CONTROL    a known event type still parses and a same-version event still
             migrates. Without this the probe could pass by breaking event
             reading outright.

Exit 0 = both hazards handled and the control holds.

Run:  python audit/evaluations/probe_event_read_forward_compat.py
"""

from __future__ import annotations

import logging
import sqlite3
import sys
import tempfile
from pathlib import Path

logging.basicConfig(level=logging.CRITICAL)

from stabilize.events.base import (  # noqa: E402
    CURRENT_SCHEMA_VERSION,
    EntityType,
    Event,
    EventMigrator,
    EventType,
)
from stabilize.events.store.sqlite import SqliteEventStore  # noqa: E402

FUTURE_TYPE = "stage.suspended.fromfuture"
WORKFLOW_ID = "wf-probe-28"


def _event() -> Event:
    return Event(
        event_type=EventType.WORKFLOW_STARTED,
        entity_type=EntityType.WORKFLOW,
        entity_id=WORKFLOW_ID,
        workflow_id=WORKFLOW_ID,
        data={"marker": "original"},
    )


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "events.db"
        store = SqliteEventStore(f"sqlite:///{db}")
        store.append(_event())
        store.append(_event())

        # Control: both events read back normally before any tampering.
        control = store.get_events_for_workflow(WORKFLOW_ID)
        if len(control) != 2:
            print(f"FAIL (control): expected 2 events, read {len(control)}")
            return 1
        print(f"control read: {len(control)} events, types={[e.event_type.value for e in control]}")

        # Simulate a newer writer: one row carries a type this reader lacks.
        raw = sqlite3.connect(db)
        raw.execute(
            "UPDATE events SET event_type = ? WHERE sequence = (SELECT MIN(sequence) FROM events)",
            (FUTURE_TYPE,),
        )
        raw.commit()
        raw.close()

        try:
            after = store.get_events_for_workflow(WORKFLOW_ID)
            read_ok, read_err = True, ""
        except Exception as exc:  # noqa: BLE001
            after, read_ok, read_err = [], False, f"{type(exc).__name__}: {exc}"

        if not read_ok:
            print(f"FINDING A: reading the workflow's events failed -> {read_err}")
            print("           one unknown row took out the whole query.")
        else:
            print(f"finding A handled: read {len(after)} events after tampering")

        # Finding B: an event from a newer schema must not be applied as-is.
        migrator = EventMigrator()
        future = Event(
            event_type=EventType.WORKFLOW_STARTED,
            entity_type=EntityType.WORKFLOW,
            entity_id=WORKFLOW_ID,
            workflow_id=WORKFLOW_ID,
            data={"marker": "from-future"},
            schema_version=CURRENT_SCHEMA_VERSION + 1,
        )
        try:
            migrator.migrate(future, strict=True)
            b_handled = False
        except Exception:  # noqa: BLE001
            b_handled = True

        # Control: a same-version event still migrates cleanly.
        same = migrator.migrate(_event(), strict=True)
        if same.schema_version != CURRENT_SCHEMA_VERSION:
            print("FAIL (control): a same-version event no longer migrates cleanly.")
            return 1

        print(
            f"finding B: future-schema event {'rejected' if b_handled else 'ACCEPTED SILENTLY'} "
            f"(schema v{future.schema_version} vs reader v{CURRENT_SCHEMA_VERSION})"
        )
        print()

        if not read_ok:
            print("FAIL (#28-A): an unknown event type breaks the entire read path.")
            return 1
        if len(after) != 2:
            print(f"FAIL (#28-A): unknown-typed row was dropped ({len(after)} of 2 read).")
            return 1
        if not b_handled:
            print("FAIL (#28-B): a newer-schema event passed through migration unchanged.")
            return 1

        print("PASS: the read path tolerates unknown types and refuses future schemas.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
