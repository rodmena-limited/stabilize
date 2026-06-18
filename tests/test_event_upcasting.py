"""Tests for event-sourcing schema upcasting on the replay read path.

The global EventMigrator is empty by default, so replay is unchanged unless
migrations are registered. These tests verify: the no-migration default is a
no-op, registered migrations are applied during replay, an incomplete registry
does not break replay (lenient), and strict vs lenient migrate() semantics.
"""

from __future__ import annotations

import pytest

from stabilize.events.base import (
    EntityType,
    Event,
    EventMetadata,
    EventType,
    get_event_migrator,
    reset_event_migrator,
)
from stabilize.events.replay import EventReplayer


def _meta() -> EventMetadata:
    return EventMetadata(correlation_id="test")


def _completed_event(seq: int, data: dict, schema_version: int = 1) -> Event:
    return Event(
        event_type=EventType.WORKFLOW_COMPLETED,
        sequence=seq,
        entity_type=EntityType.WORKFLOW,
        entity_id="wf-1",
        workflow_id="wf-1",
        version=seq,
        data=data,
        metadata=_meta(),
        schema_version=schema_version,
    )


class _StubStore:
    """Minimal EventStore stand-in exposing only what EventReplayer needs."""

    def __init__(self, events: list[Event]) -> None:
        self._events = events

    def get_events_for_workflow(self, workflow_id: str, start_sequence: int = 0) -> list[Event]:
        return [e for e in self._events if e.workflow_id == workflow_id and e.sequence > start_sequence]


class TestGlobalMigrator:
    def test_singleton_identity_and_reset(self) -> None:
        m1 = get_event_migrator()
        m2 = get_event_migrator()
        assert m1 is m2
        reset_event_migrator()
        assert get_event_migrator() is not m1


class TestReplayUpcasting:
    def test_no_migrations_replay_is_unchanged(self) -> None:
        """Default (no migrations) reconstructs state exactly as before."""
        events = [_completed_event(1, {"status": "SUCCEEDED"})]
        replayer = EventReplayer(_StubStore(events))  # type: ignore[arg-type]
        state = replayer.rebuild_workflow_state("wf-1")
        assert state["status"] == "SUCCEEDED"

    def test_registered_migration_applied_on_replay(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A v1->v2 migration upcasts historical events during replay."""
        # Simulate a future release that bumped the current schema version.
        monkeypatch.setattr("stabilize.events.base.CURRENT_SCHEMA_VERSION", 2)

        migrator = get_event_migrator()

        @migrator.register(from_version=1, to_version=2)
        def _v1_to_v2(event: Event) -> Event:
            data = dict(event.data)
            # v1 stored the result under "legacy_status"; v2 uses "status".
            data["status"] = data.pop("legacy_status", None)
            return Event(
                event_id=event.event_id,
                event_type=event.event_type,
                timestamp=event.timestamp,
                sequence=event.sequence,
                entity_type=event.entity_type,
                entity_id=event.entity_id,
                workflow_id=event.workflow_id,
                version=event.version,
                data=data,
                metadata=event.metadata,
                schema_version=2,
            )

        # Stored v1 event in the *old* shape.
        events = [_completed_event(1, {"legacy_status": "DONE_V1"}, schema_version=1)]
        replayer = EventReplayer(_StubStore(events))  # type: ignore[arg-type]
        state = replayer.rebuild_workflow_state("wf-1")

        # The migration ran: the reconstructed status comes from legacy_status.
        assert state["status"] == "DONE_V1"

    def test_incomplete_registry_does_not_break_replay(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With no migration registered, replay is lenient (no exception)."""
        monkeypatch.setattr("stabilize.events.base.CURRENT_SCHEMA_VERSION", 2)
        # No migrations registered; a v1 event should still replay.
        events = [_completed_event(1, {"status": "SUCCEEDED"}, schema_version=1)]
        replayer = EventReplayer(_StubStore(events))  # type: ignore[arg-type]
        state = replayer.rebuild_workflow_state("wf-1")  # must not raise
        assert state["status"] == "SUCCEEDED"


class TestMigrateSemantics:
    def test_strict_raises_when_path_missing(self) -> None:
        migrator = get_event_migrator()
        event = _completed_event(1, {"status": "X"}, schema_version=1)
        with pytest.raises(ValueError):
            migrator.migrate(event, target_version=2, strict=True)

    def test_lenient_returns_partial(self) -> None:
        migrator = get_event_migrator()
        event = _completed_event(1, {"status": "X"}, schema_version=1)
        # No path registered; lenient returns the event unchanged.
        result = migrator.migrate(event, target_version=2, strict=False)
        assert result is event

    def test_same_version_is_noop(self) -> None:
        migrator = get_event_migrator()
        event = _completed_event(1, {"status": "X"}, schema_version=1)
        assert migrator.migrate(event, target_version=1) is event
