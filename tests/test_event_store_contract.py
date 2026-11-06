"""EventStore declares whether it delivers in commit order; subscriptions poll by sequence when it does not (#109)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from stabilize.events.store.interface import EventQuery, EventStore
from stabilize.events.store.sqlite import SqliteEventStore
from stabilize.events.subscriptions import SubscriptionManager


class _SequenceOnlyStore(EventStore):
    def append(self, event: Any, connection: Any = None) -> Any:
        return event

    def append_batch(self, events: list[Any], connection: Any = None) -> list[Any]:
        return events

    def get_events(self, query: EventQuery) -> Iterator[Any]:
        return iter(())

    def get_events_for_entity(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    def get_events_for_workflow(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    def get_current_sequence(self) -> int:
        return 0

    def get_events_since(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []


def test_a_store_that_does_not_declare_commit_order_is_polled_by_sequence() -> None:
    store = _SequenceOnlyStore()
    assert store.supports_commit_cursor() is False
    assert SubscriptionManager(store)._uses_commit_cursor() is False


def test_the_sqlite_store_declares_commit_order(tmp_path: Path) -> None:
    store = SqliteEventStore(f"sqlite:///{tmp_path / 'events.db'}")
    assert store.supports_commit_cursor() is True
    assert SubscriptionManager(store)._uses_commit_cursor() is True
