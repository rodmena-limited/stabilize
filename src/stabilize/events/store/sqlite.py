"""
SQLite event store implementation.

Provides durable, append-only event storage using SQLite.
Events are stored with auto-incrementing sequence numbers
for global ordering.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from stabilize.events.base import EntityType, Event, EventMetadata, EventType
from stabilize.events.store.interface import EventQuery, EventStore
from stabilize.persistence.connection import get_connection_manager

# Schema for events table
EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    data TEXT NOT NULL DEFAULT '{}',
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    actor TEXT DEFAULT 'system',
    source_handler TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_type, entity_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id, sequence);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_correlation ON events(correlation_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
"""

# Schema for snapshots table
SNAPSHOTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now', 'utc')),
    UNIQUE(entity_type, entity_id, version)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_entity ON snapshots(entity_type, entity_id);
"""

# Schema for durable subscriptions
SUBSCRIPTIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS event_subscriptions (
    id TEXT PRIMARY KEY,
    event_types TEXT,
    entity_filter TEXT,
    last_sequence INTEGER DEFAULT 0,
    webhook_url TEXT,
    created_at TEXT DEFAULT (datetime('now', 'utc')),
    updated_at TEXT DEFAULT (datetime('now', 'utc'))
);
"""


class SqliteEventStore(EventStore):
    """
    SQLite implementation of event store.

    Uses the shared ConnectionManager for thread-local connections.
    Events are stored in an append-only table with auto-incrementing
    sequence numbers.

    Thread-safety:
    - Uses thread-local connections from ConnectionManager
    - Sequence assignment is atomic via SQLite's AUTOINCREMENT
    """

    def __init__(
        self,
        connection_string: str,
        create_tables: bool = True,
    ) -> None:
        """
        Initialize SQLite event store.

        Args:
            connection_string: SQLite connection string (e.g., "sqlite:///./app.db")
            create_tables: Whether to create tables if they don't exist.
        """
        self._connection_string = connection_string
        self._lock = threading.Lock()

        if create_tables:
            self._create_tables()

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local connection."""
        return get_connection_manager().get_sqlite_connection(self._connection_string)

    def _create_tables(self) -> None:
        """Create event tables if they don't exist."""
        conn = self._get_connection()
        conn.executescript(EVENTS_SCHEMA)
        conn.executescript(SNAPSHOTS_SCHEMA)
        conn.executescript(SUBSCRIPTIONS_SCHEMA)
        conn.commit()

    def append(self, event: Event, connection: Any | None = None) -> Event:
        """Append a single event to the store."""
        events = self.append_batch([event], connection)
        return events[0]

    def append_batch(self, events: list[Event], connection: Any | None = None) -> list[Event]:
        """Append multiple events atomically."""
        if not events:
            return []

        conn = connection if connection is not None else self._get_connection()
        should_commit = connection is None

        try:
            result_events = []

            for event in events:
                cursor = conn.execute(
                    """
                    INSERT INTO events (
                        event_id, event_type, timestamp, entity_type, entity_id,
                        workflow_id, version, data, correlation_id, causation_id,
                        actor, source_handler
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.event_type.value,
                        event.timestamp.isoformat(),
                        event.entity_type.value,
                        event.entity_id,
                        event.workflow_id,
                        event.version,
                        json.dumps(event.data),
                        event.metadata.correlation_id,
                        event.metadata.causation_id,
                        event.metadata.actor,
                        event.metadata.source_handler,
                    ),
                )

                # Get the assigned sequence number
                sequence = cursor.lastrowid or 0
                result_events.append(event.with_sequence(sequence))

            if should_commit:
                conn.commit()

            return result_events

        except Exception:
            if should_commit:
                conn.rollback()
            raise

    def get_events(self, query: EventQuery) -> Iterator[Event]:
        """Query events matching criteria."""
        conn = self._get_connection()

        sql_parts = ["SELECT * FROM events WHERE 1=1"]
        params: list[Any] = []

        if query.entity_type is not None:
            sql_parts.append("AND entity_type = ?")
            params.append(query.entity_type.value)

        if query.entity_id is not None:
            sql_parts.append("AND entity_id = ?")
            params.append(query.entity_id)

        if query.workflow_id is not None:
            sql_parts.append("AND workflow_id = ?")
            params.append(query.workflow_id)

        if query.event_types:
            placeholders = ",".join("?" * len(query.event_types))
            sql_parts.append(f"AND event_type IN ({placeholders})")
            params.extend(et.value for et in query.event_types)

        if query.from_sequence is not None:
            sql_parts.append("AND sequence > ?")
            params.append(query.from_sequence)

        if query.to_sequence is not None:
            sql_parts.append("AND sequence <= ?")
            params.append(query.to_sequence)

        if query.from_timestamp is not None:
            sql_parts.append("AND timestamp >= ?")
            params.append(query.from_timestamp.isoformat())

        if query.to_timestamp is not None:
            sql_parts.append("AND timestamp <= ?")
            params.append(query.to_timestamp.isoformat())

        # Ordering
        order_col = "sequence" if query.order_by == "sequence" else "timestamp"
        order_dir = "ASC" if query.ascending else "DESC"
        sql_parts.append(f"ORDER BY {order_col} {order_dir}")

        # Pagination
        sql_parts.append("LIMIT ? OFFSET ?")
        params.extend([query.limit, query.offset])

        sql = " ".join(sql_parts)

        cursor = conn.execute(sql, params)
        for row in cursor:
            yield self._row_to_event(row)

    def get_events_for_entity(
        self,
        entity_type: EntityType,
        entity_id: str,
        from_version: int = 0,
    ) -> list[Event]:
        """Get all events for a specific entity."""
        conn = self._get_connection()

        cursor = conn.execute(
            """
            SELECT * FROM events
            WHERE entity_type = ? AND entity_id = ? AND version > ?
            ORDER BY sequence ASC
            """,
            (entity_type.value, entity_id, from_version),
        )

        return [self._row_to_event(row) for row in cursor]

    def get_events_for_workflow(
        self,
        workflow_id: str,
        from_sequence: int = 0,
    ) -> list[Event]:
        """Get all events for a workflow execution."""
        conn = self._get_connection()

        cursor = conn.execute(
            """
            SELECT * FROM events
            WHERE workflow_id = ? AND sequence > ?
            ORDER BY sequence ASC
            """,
            (workflow_id, from_sequence),
        )

        return [self._row_to_event(row) for row in cursor]

    def get_current_sequence(self) -> int:
        """Get the current (latest) global sequence number."""
        conn = self._get_connection()

        cursor = conn.execute("SELECT MAX(sequence) FROM events")
        result = cursor.fetchone()
        return result[0] if result and result[0] is not None else 0

    def get_events_since(
        self,
        sequence: int,
        limit: int = 1000,
    ) -> list[Event]:
        """Get events since a sequence number."""
        conn = self._get_connection()

        cursor = conn.execute(
            """
            SELECT * FROM events
            WHERE sequence > ?
            ORDER BY sequence ASC
            LIMIT ?
            """,
            (sequence, limit),
        )

        return [self._row_to_event(row) for row in cursor]

    def get_event_by_id(self, event_id: str) -> Event | None:
        """Get a single event by its ID."""
        conn = self._get_connection()

        cursor = conn.execute(
            "SELECT * FROM events WHERE event_id = ?",
            (event_id,),
        )

        row = cursor.fetchone()
        return self._row_to_event(row) if row else None

    def count_events(self, query: EventQuery | None = None) -> int:
        """Count events matching query."""
        conn = self._get_connection()

        if query is None:
            cursor = conn.execute("SELECT COUNT(*) FROM events")
        else:
            sql_parts = ["SELECT COUNT(*) FROM events WHERE 1=1"]
            params: list[Any] = []

            if query.entity_type is not None:
                sql_parts.append("AND entity_type = ?")
                params.append(query.entity_type.value)

            if query.entity_id is not None:
                sql_parts.append("AND entity_id = ?")
                params.append(query.entity_id)

            if query.workflow_id is not None:
                sql_parts.append("AND workflow_id = ?")
                params.append(query.workflow_id)

            if query.event_types:
                placeholders = ",".join("?" * len(query.event_types))
                sql_parts.append(f"AND event_type IN ({placeholders})")
                params.extend(et.value for et in query.event_types)

            if query.from_sequence is not None:
                sql_parts.append("AND sequence > ?")
                params.append(query.from_sequence)

            if query.to_sequence is not None:
                sql_parts.append("AND sequence <= ?")
                params.append(query.to_sequence)

            sql = " ".join(sql_parts)
            cursor = conn.execute(sql, params)

        result = cursor.fetchone()
        return result[0] if result else 0

    def _row_to_event(self, row: sqlite3.Row) -> Event:
        """Convert a database row to an Event."""
        # Parse timestamp
        timestamp_str = row["timestamp"]
        if timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str[:-1] + "+00:00"
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
        except ValueError:
            timestamp = datetime.now(UTC)

        # Parse data
        try:
            data = json.loads(row["data"]) if row["data"] else {}
        except (json.JSONDecodeError, TypeError):
            data = {}

        return Event(
            event_id=row["event_id"],
            event_type=EventType(row["event_type"]),
            timestamp=timestamp,
            sequence=row["sequence"],
            entity_type=EntityType(row["entity_type"]),
            entity_id=row["entity_id"],
            workflow_id=row["workflow_id"],
            version=row["version"],
            data=data,
            metadata=EventMetadata(
                correlation_id=row["correlation_id"],
                causation_id=row["causation_id"],
                actor=row["actor"] or "system",
                source_handler=row["source_handler"],
            ),
        )

    # Snapshot methods

    def save_snapshot(
        self,
        entity_type: EntityType,
        entity_id: str,
        workflow_id: str,
        version: int,
        sequence: int,
        state: dict[str, Any],
    ) -> None:
        """Save a snapshot of entity state."""
        conn = self._get_connection()

        conn.execute(
            """
            INSERT OR REPLACE INTO snapshots
            (entity_type, entity_id, workflow_id, version, sequence, state)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                entity_type.value,
                entity_id,
                workflow_id,
                version,
                sequence,
                json.dumps(state),
            ),
        )
        conn.commit()

    def get_latest_snapshot(
        self,
        entity_type: EntityType,
        entity_id: str,
    ) -> dict[str, Any] | None:
        """Get the latest snapshot for an entity."""
        conn = self._get_connection()

        cursor = conn.execute(
            """
            SELECT * FROM snapshots
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            (entity_type.value, entity_id),
        )

        row = cursor.fetchone()
        if row is None:
            return None

        try:
            state = json.loads(row["state"]) if row["state"] else {}
        except (json.JSONDecodeError, TypeError):
            state = {}

        return {
            "entity_type": row["entity_type"],
            "entity_id": row["entity_id"],
            "workflow_id": row["workflow_id"],
            "version": row["version"],
            "sequence": row["sequence"],
            "state": state,
        }

    # Subscription methods

    def save_subscription(
        self,
        subscription_id: str,
        event_types: list[EventType] | None,
        entity_filter: dict[str, Any] | None,
        last_sequence: int,
        webhook_url: str | None = None,
    ) -> None:
        """Save or update a durable subscription."""
        conn = self._get_connection()

        event_types_json = json.dumps([et.value for et in event_types]) if event_types else None
        entity_filter_json = json.dumps(entity_filter) if entity_filter else None

        conn.execute(
            """
            INSERT OR REPLACE INTO event_subscriptions
            (id, event_types, entity_filter, last_sequence, webhook_url, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now', 'utc'))
            """,
            (
                subscription_id,
                event_types_json,
                entity_filter_json,
                last_sequence,
                webhook_url,
            ),
        )
        conn.commit()

    def get_subscription(self, subscription_id: str) -> dict[str, Any] | None:
        """Get a durable subscription by ID."""
        conn = self._get_connection()

        cursor = conn.execute(
            "SELECT * FROM event_subscriptions WHERE id = ?",
            (subscription_id,),
        )

        row = cursor.fetchone()
        if row is None:
            return None

        try:
            event_types = [EventType(et) for et in json.loads(row["event_types"])] if row["event_types"] else None
        except (json.JSONDecodeError, TypeError):
            event_types = None

        try:
            entity_filter = json.loads(row["entity_filter"]) if row["entity_filter"] else None
        except (json.JSONDecodeError, TypeError):
            entity_filter = None

        return {
            "id": row["id"],
            "event_types": event_types,
            "entity_filter": entity_filter,
            "last_sequence": row["last_sequence"],
            "webhook_url": row["webhook_url"],
        }

    def update_subscription_sequence(self, subscription_id: str, last_sequence: int) -> None:
        """Update the last processed sequence for a subscription."""
        conn = self._get_connection()

        conn.execute(
            """
            UPDATE event_subscriptions
            SET last_sequence = ?, updated_at = datetime('now', 'utc')
            WHERE id = ?
            """,
            (last_sequence, subscription_id),
        )
        conn.commit()

    def delete_subscription(self, subscription_id: str) -> None:
        """Delete a durable subscription."""
        conn = self._get_connection()

        conn.execute(
            "DELETE FROM event_subscriptions WHERE id = ?",
            (subscription_id,),
        )
        conn.commit()

    def list_subscriptions(self) -> list[dict[str, Any]]:
        """List all durable subscriptions."""
        conn = self._get_connection()

        cursor = conn.execute("SELECT id, last_sequence FROM event_subscriptions")

        return [{"id": row["id"], "last_sequence": row["last_sequence"]} for row in cursor]
