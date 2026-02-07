"""
Event append, query, and retrieval methods for PostgreSQL event store.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from stabilize.events.base import EntityType, Event, EventMetadata, EventType
from stabilize.events.store.interface import EventQuery

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool


class PostgresEventsMixin:
    """Mixin providing event append, query, and retrieval methods."""

    if TYPE_CHECKING:
        _pool: ConnectionPool

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
                count_val = row[0] if isinstance(row, tuple) else row.get("count", 0)
                return count_val if count_val is not None else 0

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
