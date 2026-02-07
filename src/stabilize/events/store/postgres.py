"""
PostgreSQL event store implementation.

Provides durable, append-only event storage using PostgreSQL.
Uses BIGSERIAL for sequence numbers and JSONB for event data.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from stabilize.events.base import EntityType, Event, EventMetadata, EventType
from stabilize.events.store.interface import EventQuery, EventStore
from stabilize.persistence.connection import get_connection_manager

# Schema for events table (PostgreSQL)
EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    sequence BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(26) NOT NULL UNIQUE,
    event_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(26) NOT NULL,
    workflow_id VARCHAR(26) NOT NULL,
    version INTEGER NOT NULL,
    data JSONB NOT NULL DEFAULT '{}',
    correlation_id VARCHAR(36) NOT NULL,
    causation_id VARCHAR(26),
    actor VARCHAR(255) DEFAULT 'system',
    source_handler VARCHAR(100)
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
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(26) NOT NULL,
    workflow_id VARCHAR(26) NOT NULL,
    version INTEGER NOT NULL,
    sequence BIGINT NOT NULL,
    state JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(entity_type, entity_id, version)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_entity ON snapshots(entity_type, entity_id);
"""

# Schema for durable subscriptions
SUBSCRIPTIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS event_subscriptions (
    id VARCHAR(100) PRIMARY KEY,
    event_types TEXT[],
    entity_filter JSONB,
    last_sequence BIGINT DEFAULT 0,
    webhook_url VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""


class PostgresEventStore(EventStore):
    """
    PostgreSQL implementation of event store.

    Uses connection pooling via ConnectionManager.
    Events are stored with BIGSERIAL sequence numbers for
    global ordering and JSONB for efficient data storage.

    Thread-safety:
    - Uses connection pool from ConnectionManager
    - Each operation gets its own connection
    - Sequence assignment is atomic via BIGSERIAL
    """

    def __init__(
        self,
        connection_string: str,
        create_tables: bool = True,
    ) -> None:
        """
        Initialize PostgreSQL event store.

        Args:
            connection_string: PostgreSQL connection string
            create_tables: Whether to create tables if they don't exist.
        """
        self._connection_string = connection_string
        self._manager = get_connection_manager()
        self._pool = self._manager.get_postgres_pool(connection_string)
        self._lock = threading.Lock()

        if create_tables:
            self._create_tables()

    def _create_tables(self) -> None:
        """Create event tables if they don't exist."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(EVENTS_SCHEMA)
                cur.execute(SNAPSHOTS_SCHEMA)
                cur.execute(SUBSCRIPTIONS_SCHEMA)
            conn.commit()

    def append(self, event: Event, connection: Any | None = None) -> Event:
        """Append a single event to the store."""
        events = self.append_batch([event], connection)
        return events[0]

    def append_batch(self, events: list[Event], connection: Any | None = None) -> list[Event]:
        """Append multiple events atomically."""
        if not events:
            return []

        def _do_append(cur: Any) -> list[Event]:
            result_events = []

            for event in events:
                cur.execute(
                    """
                    INSERT INTO events (
                        event_id, event_type, timestamp, entity_type, entity_id,
                        workflow_id, version, data, correlation_id, causation_id,
                        actor, source_handler
                    ) VALUES (
                        %(event_id)s, %(event_type)s, %(timestamp)s, %(entity_type)s,
                        %(entity_id)s, %(workflow_id)s, %(version)s, %(data)s,
                        %(correlation_id)s, %(causation_id)s, %(actor)s, %(source_handler)s
                    )
                    RETURNING sequence
                    """,
                    {
                        "event_id": event.event_id,
                        "event_type": event.event_type.value,
                        "timestamp": event.timestamp,
                        "entity_type": event.entity_type.value,
                        "entity_id": event.entity_id,
                        "workflow_id": event.workflow_id,
                        "version": event.version,
                        "data": json.dumps(event.data),
                        "correlation_id": event.metadata.correlation_id,
                        "causation_id": event.metadata.causation_id,
                        "actor": event.metadata.actor,
                        "source_handler": event.metadata.source_handler,
                    },
                )

                row = cur.fetchone()
                sequence = row[0] if isinstance(row, tuple) else row.get("sequence", 0)
                result_events.append(event.with_sequence(sequence))

            return result_events

        if connection is not None:
            with connection.cursor() as cur:
                return _do_append(cur)
        else:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    result = _do_append(cur)
                conn.commit()
                return result

    def get_events(self, query: EventQuery) -> Iterator[Event]:
        """Query events matching criteria."""
        sql_parts = ["SELECT * FROM events WHERE 1=1"]
        params: dict[str, Any] = {}

        if query.entity_type is not None:
            sql_parts.append("AND entity_type = %(entity_type)s")
            params["entity_type"] = query.entity_type.value

        if query.entity_id is not None:
            sql_parts.append("AND entity_id = %(entity_id)s")
            params["entity_id"] = query.entity_id

        if query.workflow_id is not None:
            sql_parts.append("AND workflow_id = %(workflow_id)s")
            params["workflow_id"] = query.workflow_id

        if query.event_types:
            sql_parts.append("AND event_type = ANY(%(event_types)s)")
            params["event_types"] = [et.value for et in query.event_types]

        if query.from_sequence is not None:
            sql_parts.append("AND sequence > %(from_sequence)s")
            params["from_sequence"] = query.from_sequence

        if query.to_sequence is not None:
            sql_parts.append("AND sequence <= %(to_sequence)s")
            params["to_sequence"] = query.to_sequence

        if query.from_timestamp is not None:
            sql_parts.append("AND timestamp >= %(from_timestamp)s")
            params["from_timestamp"] = query.from_timestamp

        if query.to_timestamp is not None:
            sql_parts.append("AND timestamp <= %(to_timestamp)s")
            params["to_timestamp"] = query.to_timestamp

        # Ordering
        order_col = "sequence" if query.order_by == "sequence" else "timestamp"
        order_dir = "ASC" if query.ascending else "DESC"
        sql_parts.append(f"ORDER BY {order_col} {order_dir}")

        # Pagination
        sql_parts.append("LIMIT %(limit)s OFFSET %(offset)s")
        params["limit"] = query.limit
        params["offset"] = query.offset

        sql = " ".join(sql_parts)

        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                for row in cur.fetchall():
                    yield self._row_to_event(row)

    def get_events_for_entity(
        self,
        entity_type: EntityType,
        entity_id: str,
        from_version: int = 0,
    ) -> list[Event]:
        """Get all events for a specific entity."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM events
                    WHERE entity_type = %(entity_type)s
                      AND entity_id = %(entity_id)s
                      AND version > %(from_version)s
                    ORDER BY sequence ASC
                    """,
                    {
                        "entity_type": entity_type.value,
                        "entity_id": entity_id,
                        "from_version": from_version,
                    },
                )
                return [self._row_to_event(row) for row in cur.fetchall()]

    def get_events_for_workflow(
        self,
        workflow_id: str,
        from_sequence: int = 0,
    ) -> list[Event]:
        """Get all events for a workflow execution."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM events
                    WHERE workflow_id = %(workflow_id)s
                      AND sequence > %(from_sequence)s
                    ORDER BY sequence ASC
                    """,
                    {"workflow_id": workflow_id, "from_sequence": from_sequence},
                )
                return [self._row_to_event(row) for row in cur.fetchall()]

    def get_current_sequence(self) -> int:
        """Get the current (latest) global sequence number."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(sequence) FROM events")
                row = cur.fetchone()
                if row is None:
                    return 0
                value = row[0] if isinstance(row, tuple) else row.get("max")
                return value if value is not None else 0

    def get_events_since(
        self,
        sequence: int,
        limit: int = 1000,
    ) -> list[Event]:
        """Get events since a sequence number."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM events
                    WHERE sequence > %(sequence)s
                    ORDER BY sequence ASC
                    LIMIT %(limit)s
                    """,
                    {"sequence": sequence, "limit": limit},
                )
                return [self._row_to_event(row) for row in cur.fetchall()]

    def get_event_by_id(self, event_id: str) -> Event | None:
        """Get a single event by its ID."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM events WHERE event_id = %(event_id)s",
                    {"event_id": event_id},
                )
                row = cur.fetchone()
                return self._row_to_event(row) if row else None

    def count_events(self, query: EventQuery | None = None) -> int:
        """Count events matching query."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                if query is None:
                    cur.execute("SELECT COUNT(*) FROM events")
                else:
                    sql_parts = ["SELECT COUNT(*) FROM events WHERE 1=1"]
                    params: dict[str, Any] = {}

                    if query.entity_type is not None:
                        sql_parts.append("AND entity_type = %(entity_type)s")
                        params["entity_type"] = query.entity_type.value

                    if query.entity_id is not None:
                        sql_parts.append("AND entity_id = %(entity_id)s")
                        params["entity_id"] = query.entity_id

                    if query.workflow_id is not None:
                        sql_parts.append("AND workflow_id = %(workflow_id)s")
                        params["workflow_id"] = query.workflow_id

                    if query.event_types:
                        sql_parts.append("AND event_type = ANY(%(event_types)s)")
                        params["event_types"] = [et.value for et in query.event_types]

                    if query.from_sequence is not None:
                        sql_parts.append("AND sequence > %(from_sequence)s")
                        params["from_sequence"] = query.from_sequence

                    if query.to_sequence is not None:
                        sql_parts.append("AND sequence <= %(to_sequence)s")
                        params["to_sequence"] = query.to_sequence

                    sql = " ".join(sql_parts)
                    cur.execute(sql, params)

                row = cur.fetchone()
                if row is None:
                    return 0
                return row[0] if isinstance(row, tuple) else row.get("count", 0)

    def _row_to_event(self, row: Any) -> Event:
        """Convert a database row to an Event."""
        # Handle both dict and tuple rows
        if isinstance(row, dict):
            data = row
        else:
            # Assume tuple with column order matching schema
            data = {
                "sequence": row[0],
                "event_id": row[1],
                "event_type": row[2],
                "timestamp": row[3],
                "entity_type": row[4],
                "entity_id": row[5],
                "workflow_id": row[6],
                "version": row[7],
                "data": row[8],
                "correlation_id": row[9],
                "causation_id": row[10],
                "actor": row[11],
                "source_handler": row[12],
            }

        # Parse timestamp
        timestamp = data["timestamp"]
        if isinstance(timestamp, str):
            if timestamp.endswith("Z"):
                timestamp = timestamp[:-1] + "+00:00"
            timestamp = datetime.fromisoformat(timestamp)
        elif timestamp is None:
            timestamp = datetime.now(UTC)

        # Parse event data
        event_data = data["data"]
        if isinstance(event_data, str):
            try:
                event_data = json.loads(event_data)
            except (json.JSONDecodeError, TypeError):
                event_data = {}
        elif event_data is None:
            event_data = {}

        return Event(
            event_id=data["event_id"],
            event_type=EventType(data["event_type"]),
            timestamp=timestamp,
            sequence=data["sequence"],
            entity_type=EntityType(data["entity_type"]),
            entity_id=data["entity_id"],
            workflow_id=data["workflow_id"],
            version=data["version"],
            data=event_data,
            metadata=EventMetadata(
                correlation_id=data["correlation_id"],
                causation_id=data["causation_id"],
                actor=data["actor"] or "system",
                source_handler=data["source_handler"],
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
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO snapshots
                    (entity_type, entity_id, workflow_id, version, sequence, state)
                    VALUES (%(entity_type)s, %(entity_id)s, %(workflow_id)s,
                            %(version)s, %(sequence)s, %(state)s)
                    ON CONFLICT (entity_type, entity_id, version)
                    DO UPDATE SET
                        sequence = EXCLUDED.sequence,
                        state = EXCLUDED.state,
                        created_at = NOW()
                    """,
                    {
                        "entity_type": entity_type.value,
                        "entity_id": entity_id,
                        "workflow_id": workflow_id,
                        "version": version,
                        "sequence": sequence,
                        "state": json.dumps(state),
                    },
                )
            conn.commit()

    def get_latest_snapshot(
        self,
        entity_type: EntityType,
        entity_id: str,
    ) -> dict[str, Any] | None:
        """Get the latest snapshot for an entity."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM snapshots
                    WHERE entity_type = %(entity_type)s AND entity_id = %(entity_id)s
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                    {"entity_type": entity_type.value, "entity_id": entity_id},
                )

                row = cur.fetchone()
                if row is None:
                    return None

                if isinstance(row, dict):
                    data = row
                else:
                    data = {
                        "entity_type": row[1],
                        "entity_id": row[2],
                        "workflow_id": row[3],
                        "version": row[4],
                        "sequence": row[5],
                        "state": row[6],
                    }

                state = data["state"]
                if isinstance(state, str):
                    try:
                        state = json.loads(state)
                    except (json.JSONDecodeError, TypeError):
                        state = {}

                return {
                    "entity_type": data["entity_type"],
                    "entity_id": data["entity_id"],
                    "workflow_id": data["workflow_id"],
                    "version": data["version"],
                    "sequence": data["sequence"],
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
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                event_types_arr = [et.value for et in event_types] if event_types else None

                cur.execute(
                    """
                    INSERT INTO event_subscriptions
                    (id, event_types, entity_filter, last_sequence, webhook_url, updated_at)
                    VALUES (%(id)s, %(event_types)s, %(entity_filter)s,
                            %(last_sequence)s, %(webhook_url)s, NOW())
                    ON CONFLICT (id)
                    DO UPDATE SET
                        event_types = EXCLUDED.event_types,
                        entity_filter = EXCLUDED.entity_filter,
                        last_sequence = EXCLUDED.last_sequence,
                        webhook_url = EXCLUDED.webhook_url,
                        updated_at = NOW()
                    """,
                    {
                        "id": subscription_id,
                        "event_types": event_types_arr,
                        "entity_filter": (json.dumps(entity_filter) if entity_filter else None),
                        "last_sequence": last_sequence,
                        "webhook_url": webhook_url,
                    },
                )
            conn.commit()

    def get_subscription(self, subscription_id: str) -> dict[str, Any] | None:
        """Get a durable subscription by ID."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM event_subscriptions WHERE id = %(id)s",
                    {"id": subscription_id},
                )

                row = cur.fetchone()
                if row is None:
                    return None

                if isinstance(row, dict):
                    data = row
                else:
                    data = {
                        "id": row[0],
                        "event_types": row[1],
                        "entity_filter": row[2],
                        "last_sequence": row[3],
                        "webhook_url": row[4],
                    }

                event_types = None
                if data["event_types"]:
                    event_types = [EventType(et) for et in data["event_types"]]

                entity_filter = data["entity_filter"]
                if isinstance(entity_filter, str):
                    try:
                        entity_filter = json.loads(entity_filter)
                    except (json.JSONDecodeError, TypeError):
                        entity_filter = None

                return {
                    "id": data["id"],
                    "event_types": event_types,
                    "entity_filter": entity_filter,
                    "last_sequence": data["last_sequence"],
                    "webhook_url": data["webhook_url"],
                }

    def update_subscription_sequence(self, subscription_id: str, last_sequence: int) -> None:
        """Update the last processed sequence for a subscription."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE event_subscriptions
                    SET last_sequence = %(last_sequence)s, updated_at = NOW()
                    WHERE id = %(id)s
                    """,
                    {"id": subscription_id, "last_sequence": last_sequence},
                )
            conn.commit()

    def delete_subscription(self, subscription_id: str) -> None:
        """Delete a durable subscription."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM event_subscriptions WHERE id = %(id)s",
                    {"id": subscription_id},
                )
            conn.commit()

    def list_subscriptions(self) -> list[dict[str, Any]]:
        """List all durable subscriptions."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, last_sequence FROM event_subscriptions")
                return [
                    {
                        "id": row[0] if isinstance(row, tuple) else row["id"],
                        "last_sequence": (row[1] if isinstance(row, tuple) else row["last_sequence"]),
                    }
                    for row in cur.fetchall()
                ]

    def close(self) -> None:
        """Close the connection pool."""
        self._manager.close_postgres_pool(self._connection_string)
