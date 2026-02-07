"""
PostgreSQL event store implementation.

Provides durable, append-only event storage using PostgreSQL.
Uses BIGSERIAL for sequence numbers and JSONB for event data.
"""

from stabilize.events.store.postgres.events import PostgresEventsMixin
from stabilize.events.store.postgres.schema import (
    EVENTS_SCHEMA,
    SNAPSHOTS_SCHEMA,
    SUBSCRIPTIONS_SCHEMA,
)
from stabilize.events.store.postgres.snapshots import PostgresSnapshotsMixin
from stabilize.events.store.postgres.store import PostgresEventStore
from stabilize.events.store.postgres.subscriptions import PostgresSubscriptionsMixin

__all__ = [
    "PostgresEventStore",
    "PostgresEventsMixin",
    "PostgresSnapshotsMixin",
    "PostgresSubscriptionsMixin",
    "EVENTS_SCHEMA",
    "SNAPSHOTS_SCHEMA",
    "SUBSCRIPTIONS_SCHEMA",
]
