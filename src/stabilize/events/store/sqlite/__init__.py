"""
SQLite event store implementation.

Provides durable, append-only event storage using SQLite.
Events are stored with auto-incrementing sequence numbers
for global ordering.
"""

from stabilize.events.store.sqlite.schema import (
    EVENTS_SCHEMA,
    SNAPSHOTS_SCHEMA,
    SUBSCRIPTIONS_SCHEMA,
)
from stabilize.events.store.sqlite.store import SqliteEventStore

__all__ = [
    "EVENTS_SCHEMA",
    "SNAPSHOTS_SCHEMA",
    "SUBSCRIPTIONS_SCHEMA",
    "SqliteEventStore",
]
