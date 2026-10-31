"""Durable subscriptions: the batch boundary, the start position, the persisted enable flag and event purge (B07)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize.events.base import EntityType, Event, EventType
from stabilize.events.subscriptions import SubscriptionManager
from tests.mg_support import output, stabilize


@pytest.fixture
def event_store(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Any]:
    if backend == "sqlite":
        from stabilize.events.store.sqlite.store import SqliteEventStore

        yield SqliteEventStore(f"sqlite:///{tmp_path / 'events.db'}", create_tables=True)
        return
    pytest.importorskip("psycopg")
    from stabilize.events.store.postgres.store import PostgresEventStore
    from tests.mg_support import scratch_database

    container = request.getfixturevalue("postgres_container")
    with scratch_database(container, "subs") as url:
        store = PostgresEventStore(url, create_tables=True)
        try:
            yield store
        finally:
            store.close()


def _event(workflow_id: str) -> Event:
    return Event(
        event_type=EventType.WORKFLOW_STARTED,
        entity_type=EntityType.WORKFLOW,
        entity_id=workflow_id,
        workflow_id=workflow_id,
        version=1,
        data={},
    )


def _append(store: Any, count: int, prefix: str = "wf") -> list[int]:
    return [store.append(_event(f"{prefix}-{i}")).sequence for i in range(count)]


def _poll(manager: SubscriptionManager, times: int = 1) -> None:
    for _ in range(times):
        manager._process_pending_events()


def test_a_full_batch_does_not_skip_the_rest(event_store: Any) -> None:
    sequences = _append(event_store, 5)
    assert sequences == [1, 2, 3, 4, 5]
    manager = SubscriptionManager(event_store, batch_size=2)
    seen: list[Event] = []
    manager.create_subscription("full-batch", seen.append, start_from="beginning")

    _poll(manager)
    assert [e.sequence for e in seen] == [1, 2]
    after_first = manager.get_subscription_status("full-batch")
    assert after_first is not None

    _poll(manager, times=4)
    assert [e.sequence for e in seen] == sequences, "events after the first full batch were skipped"
    assert after_first["last_commit_sequence"] == 2, "a full batch must leave the cursor on its last event"

    reloaded = SubscriptionManager(event_store, batch_size=2)
    later: list[Event] = []
    assert reloaded.load_subscription("full-batch", later.append)
    _append(event_store, 1, prefix="late")
    _poll(reloaded, times=2)
    assert [e.sequence for e in later] == [6], "the reloaded cursor re-delivered or skipped events"


def test_latest_starts_after_the_events_already_stored(event_store: Any) -> None:
    _append(event_store, 3)
    manager = SubscriptionManager(event_store, batch_size=10)
    seen: list[Event] = []
    manager.create_subscription("latest", seen.append, start_from="latest")
    _append(event_store, 1, prefix="new")

    _poll(manager, times=2)
    assert [e.sequence for e in seen] == [4], "a subscription created at 'latest' replayed history"


def test_a_sequence_start_delivers_everything_after_it(event_store: Any) -> None:
    _append(event_store, 5)
    manager = SubscriptionManager(event_store, batch_size=10)
    seen: list[Event] = []
    manager.create_subscription("from-three", seen.append, start_from="3")

    _poll(manager, times=2)
    assert [e.sequence for e in seen] == [4, 5]


def test_a_subscription_without_a_commit_cursor_resumes_after_its_sequence(event_store: Any) -> None:
    _append(event_store, 5)
    event_store.save_subscription(
        subscription_id="pre-cursor",
        event_types=None,
        entity_filter=None,
        last_sequence=3,
    )
    manager = SubscriptionManager(event_store, batch_size=10)
    seen: list[Event] = []
    assert manager.load_subscription("pre-cursor", seen.append)

    _poll(manager, times=2)
    assert [e.sequence for e in seen] == [4, 5], "a subscription that only ever had a sequence cursor replayed history"


def test_a_disabled_subscription_stays_disabled_after_a_reload(event_store: Any) -> None:
    _append(event_store, 1)

    def failing(event: Event) -> None:
        raise RuntimeError("handler down")

    manager = SubscriptionManager(event_store, batch_size=10)
    manager.create_subscription("flaky", failing, start_from="beginning")
    manager._subscriptions["flaky"].max_errors = 1
    _poll(manager)
    first = manager.get_subscription_status("flaky")
    assert first is not None and first["enabled"] is False

    again = SubscriptionManager(event_store, batch_size=10)
    assert again.load_subscription("flaky", failing)
    second = again.get_subscription_status("flaky")
    assert second is not None
    assert second["enabled"] is False, "the disable did not survive a reload"

    assert again.enable_subscription("flaky") is True
    third = SubscriptionManager(event_store, batch_size=10)
    assert third.load_subscription("flaky", failing)
    status = third.get_subscription_status("flaky")
    assert status is not None
    assert status["enabled"] is True and status["error_count"] == 0
    assert third.enable_subscription("absent") is False


def test_purge_refuses_while_an_enabled_subscription_is_behind(event_store: Any) -> None:
    from stabilize.events import SubscriptionBehindError

    _append(event_store, 5)
    manager = SubscriptionManager(event_store, batch_size=2)
    seen: list[Event] = []
    manager.create_subscription("slow", seen.append, start_from="beginning")
    _poll(manager)
    assert [e.sequence for e in seen] == [1, 2]

    with pytest.raises(SubscriptionBehindError, match="slow"):
        event_store.purge_events(before_sequence=4)
    assert event_store.count_events() == 5, "a refused purge deleted events"

    _poll(manager, times=4)
    assert [e.sequence for e in seen] == [1, 2, 3, 4, 5]
    assert event_store.purge_events(before_sequence=4) == 3
    assert [e.sequence for e in event_store.get_events_since(0)] == [4, 5]

    def failing(event: Event) -> None:
        raise RuntimeError("handler down")

    manager.create_subscription("stuck", failing, start_from="beginning")
    manager._subscriptions["stuck"].max_errors = 1
    _poll(manager)
    assert event_store.purge_events(before_sequence=5) == 1, "a disabled subscription held the purge back"

    manager.create_subscription("lagging", seen.append, start_from="beginning")
    with pytest.raises(SubscriptionBehindError, match="lagging"):
        event_store.purge_events(before_sequence=6)
    assert event_store.purge_events(before_sequence=6, ignore_subscriptions=True) == 1
    assert event_store.count_events() == 0


def test_purge_by_time_and_the_empty_call(event_store: Any) -> None:
    _append(event_store, 3)
    with pytest.raises(ValueError):
        event_store.purge_events()
    assert event_store.purge_events(before=datetime.now(UTC) - timedelta(days=1)) == 0
    assert event_store.purge_events(before=datetime.now(UTC) + timedelta(days=1), batch_size=2) == 3
    assert event_store.count_events() == 0


def test_the_cli_purges_events_and_counts_first(tmp_path: Path) -> None:
    from stabilize.events.store.sqlite.store import SqliteEventStore

    url = f"sqlite:///{tmp_path / 'cli.db'}"
    store = SqliteEventStore(url, create_tables=True)
    _append(store, 3)

    dry = stabilize("purge-events", "--db-url", url, "--before-sequence", "3", "--dry-run")
    assert dry.returncode == 0, output(dry)
    assert "2 event(s)" in dry.stdout, output(dry)
    assert store.count_events() == 3

    wet = stabilize("purge-events", "--db-url", url, "--before-sequence", "3")
    assert wet.returncode == 0, output(wet)
    assert "Purged 2 event(s)" in wet.stdout, output(wet)
    assert [e.sequence for e in store.get_events_since(0)] == [3]

    neither = stabilize("purge-events", "--db-url", url)
    assert neither.returncode != 0
