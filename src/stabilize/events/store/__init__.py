"""Event store implementations.

Provides append-only storage for events with support for
querying by entity, workflow, time range, and event type.
"""

from __future__ import annotations

from stabilize.events.store.interface import AppendResult, EventQuery, EventStore
from stabilize.events.store.retention import SubscriptionBehindError
from stabilize.events.store.sqlite import SqliteEventStore

__all__ = [
    "AppendResult",
    "EventQuery",
    "EventStore",
    "SqliteEventStore",
    "SubscriptionBehindError",
]

# PostgreSQL support is optional (requires psycopg)
try:
    from stabilize.events.store.postgres import PostgresEventStore

    __all__ += ["PostgresEventStore"]
except ImportError:
    pass
