"""
Repro tests for event-subsystem audit findings A2-12/13/14.

- Metrics projection inflated by redundant appends of the same logical
  transition (dedup was on the raw global sequence).
- Replay warned about "sequence gaps" using the GLOBAL sequence, which is
  interleaved across workflows — spurious warnings on every multi-workflow
  replay.
- ASYNC bus delivery via a shared thread pool had no per-subscriber ordering.
"""

import logging
import threading
import time

from stabilize.events import get_event_bus, reset_event_bus
from stabilize.events.base import EntityType, Event, EventType
from stabilize.events.bus import SubscriptionMode
from stabilize.events.projections.metrics import StageMetricsProjection


def _stage_event(
    event_type: EventType,
    sequence: int,
    version: int = 1,
    entity_id: str = "stage-1",
    stage_type: str = "shell",
) -> Event:
    event = Event(
        event_type=event_type,
        entity_type=EntityType.STAGE,
        entity_id=entity_id,
        workflow_id="wf-1",
        data={"type": stage_type},
        version=version,
    )
    return event.with_sequence(sequence)


class TestMetricsLogicalDedup:
    def test_redundant_append_of_same_transition_counts_once(self) -> None:
        """The same logical transition (entity, type, version) appended twice
        with different global sequences must count once."""
        projection = StageMetricsProjection()
        projection.apply(_stage_event(EventType.STAGE_STARTED, sequence=10, version=2))
        projection.apply(_stage_event(EventType.STAGE_STARTED, sequence=11, version=2))

        assert projection._stage_metrics["shell"].execution_count == 1, (
            "duplicate append of the same logical transition inflated metrics"
        )

    def test_distinct_transitions_still_count(self) -> None:
        """Retry-loop iterations carry a new entity version and must count."""
        projection = StageMetricsProjection()
        projection.apply(_stage_event(EventType.STAGE_STARTED, sequence=10, version=2))
        projection.apply(_stage_event(EventType.STAGE_STARTED, sequence=20, version=5))

        assert projection._stage_metrics["shell"].execution_count == 2


class TestReplayNoSpuriousGapWarnings:
    def test_interleaved_global_sequences_do_not_warn(self, caplog, tmp_path) -> None:
        """Workflow A holding global sequences 1,4,7 (B holds 2,3,5,6) is a
        complete log; replay must not warn about missing events."""
        from stabilize.events import SqliteEventStore
        from stabilize.events.replay import EventReplayer

        store = SqliteEventStore(f"sqlite:///{tmp_path}/replay_gaps.db", create_tables=True)

        # Interleave two workflows in the global sequence: append order
        # a, b, b, a, b, b, a gives wf-a the gappy global sequences 1, 4, 7.
        for wf, idx in (("wf-a", 0), ("wf-b", 0), ("wf-b", 1), ("wf-a", 1), ("wf-b", 2), ("wf-b", 3), ("wf-a", 2)):
            store.append(
                Event(
                    event_type=EventType.WORKFLOW_STARTED if idx == 0 else EventType.STAGE_COMPLETED,
                    entity_type=EntityType.WORKFLOW if idx == 0 else EntityType.STAGE,
                    entity_id=wf if idx == 0 else f"{wf}-stage-{idx}",
                    workflow_id=wf,
                    data={"application": "t", "name": wf, "type": "shell"},
                    version=idx,
                )
            )

        replayer = EventReplayer(store)
        with caplog.at_level(logging.WARNING):
            replayer.rebuild_workflow_state("wf-a")
        gap_warnings = [r for r in caplog.records if "gap" in r.getMessage().lower()]
        assert gap_warnings == [], (
            f"spurious gap warnings on a complete per-workflow log: "
            f"{[r.getMessage() for r in gap_warnings]}"
        )


class TestAsyncBusOrdering:
    def test_async_subscriber_receives_events_in_publish_order(self) -> None:
        reset_event_bus()
        try:
            bus = get_event_bus()
            received: list[str] = []
            done = threading.Event()
            first_seen = threading.Event()

            def slow_then_fast(event: Event) -> None:
                if not first_seen.is_set():
                    first_seen.set()
                    time.sleep(0.15)  # delay the FIRST event's handling
                received.append(event.entity_id)
                if len(received) == 2:
                    done.set()

            bus.subscribe("ordered-sub", slow_then_fast, mode=SubscriptionMode.ASYNC)

            bus.publish(_stage_event(EventType.STAGE_STARTED, sequence=1, entity_id="first"))
            bus.publish(_stage_event(EventType.STAGE_STARTED, sequence=2, entity_id="second"))

            assert done.wait(5.0), "async deliveries never completed"
            assert received == ["first", "second"], (
                f"ASYNC delivery reordered events for one subscriber: {received}"
            )
        finally:
            reset_event_bus()
