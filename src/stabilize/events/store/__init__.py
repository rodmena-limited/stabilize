"""
Event store implementations.

Provides append-only storage for events with support for
querying by entity, workflow, time range, and event type.
"""

from stabilize.events.store.interface import AppendResult, EventQuery, EventStore
from stabilize.events.store.memory import InMemoryEventStore
from stabilize.events.store.sqlite import SqliteEventStore

__all__ = [
    "EventStore",
    "EventQuery",
    "AppendResult",
    "InMemoryEventStore",
    "SqliteEventStore",
]

# PostgreSQL support is optional (requires psycopg)
try:
    from stabilize.events.store.postgres import PostgresEventStore  # noqa: F401

    __all__.append("PostgresEventStore")
except ImportError:
    pass
