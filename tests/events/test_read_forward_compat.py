"""Ticket #28: the event read path must tolerate what a newer build wrote.

An unrecognised event_type previously raised inside row->Event conversion, which
failed the entire query rather than one row; and an event from a newer schema
fell through migration unchanged and was applied with the wrong layout. Both are
prerequisites for ever adding an EventType or bumping the schema.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from stabilize.events.base import (
    CURRENT_SCHEMA_VERSION,
    RAW_EVENT_TYPE,
    EntityType,
    Event,
    EventMigrator,
    EventType,
    parse_event_type,
)
from stabilize.events.store.sqlite import SqliteEventStore

WORKFLOW_ID = "wf-28"
FUTURE_TYPE = "stage.suspended.fromfuture"


def _event(marker: str = "m", schema_version: int = CURRENT_SCHEMA_VERSION) -> Event:
    return Event(
        event_type=EventType.WORKFLOW_STARTED,
        entity_type=EntityType.WORKFLOW,
        entity_id=WORKFLOW_ID,
        workflow_id=WORKFLOW_ID,
        data={"marker": marker},
        schema_version=schema_version,
    )


@pytest.fixture
def store(tmp_path: Path) -> SqliteEventStore:
    return SqliteEventStore(f"sqlite:///{tmp_path / 'events.db'}")


def _retype_first_row(tmp_path: Path, value: str) -> None:
    raw = sqlite3.connect(tmp_path / "events.db")
    raw.execute(
        "UPDATE events SET event_type = ? WHERE sequence = (SELECT MIN(sequence) FROM events)",
        (value,),
    )
    raw.commit()
    raw.close()


def test_unknown_event_type_does_not_fail_the_query(store, tmp_path: Path) -> None:
    store.append(_event("first"))
    store.append(_event("second"))
    _retype_first_row(tmp_path, FUTURE_TYPE)

    events = store.get_events_for_workflow(WORKFLOW_ID)

    assert len(events) == 2
    unknown = [e for e in events if e.event_type is EventType.UNKNOWN]
    assert len(unknown) == 1
    assert unknown[0].data[RAW_EVENT_TYPE] == FUTURE_TYPE


def test_known_event_types_still_parse(store, tmp_path: Path) -> None:
    """Control: tolerance must not degrade recognised types."""
    store.append(_event("first"))
    store.append(_event("second"))

    events = store.get_events_for_workflow(WORKFLOW_ID)

    assert len(events) == 2
    assert {e.event_type for e in events} == {EventType.WORKFLOW_STARTED}
    assert all(RAW_EVENT_TYPE not in e.data for e in events)


def test_parse_event_type_maps_unknown_and_preserves_known() -> None:
    assert parse_event_type("workflow.started") is EventType.WORKFLOW_STARTED
    assert parse_event_type("no.such.type") is EventType.UNKNOWN


def test_future_schema_event_is_refused_under_strict() -> None:
    migrator = EventMigrator()
    future = _event(schema_version=CURRENT_SCHEMA_VERSION + 1)

    with pytest.raises(ValueError, match="newer than this build"):
        migrator.migrate(future, strict=True)


def test_same_version_event_still_migrates() -> None:
    """Control: the future-schema guard must not block ordinary migration."""
    migrator = EventMigrator()
    migrated = migrator.migrate(_event(), strict=True)

    assert migrated.schema_version == CURRENT_SCHEMA_VERSION


def test_replay_skips_a_future_schema_event() -> None:
    from stabilize.events.replay import EventReplayer, WorkflowState

    replayer = EventReplayer(event_store=None)  # type: ignore[arg-type]
    state = WorkflowState(workflow_id=WORKFLOW_ID)
    before = state.status

    future = Event(
        event_type=EventType.WORKFLOW_COMPLETED,
        entity_type=EntityType.WORKFLOW,
        entity_id=WORKFLOW_ID,
        workflow_id=WORKFLOW_ID,
        data={},
        schema_version=CURRENT_SCHEMA_VERSION + 1,
    )
    replayer._apply_event(state, future)

    assert state.status == before


def test_replay_applies_a_current_schema_event() -> None:
    """Control: the skip must not swallow ordinary events."""
    from stabilize.events.replay import EventReplayer, WorkflowState

    replayer = EventReplayer(event_store=None)  # type: ignore[arg-type]
    state = WorkflowState(workflow_id=WORKFLOW_ID)

    replayer._apply_event(
        state,
        Event(
            event_type=EventType.WORKFLOW_COMPLETED,
            entity_type=EntityType.WORKFLOW,
            entity_id=WORKFLOW_ID,
            workflow_id=WORKFLOW_ID,
            data={},
        ),
    )

    assert state.status != WorkflowState(workflow_id=WORKFLOW_ID).status
