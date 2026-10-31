"""
Event append, query, and retrieval methods for SQLite event store.

Provides the SqliteEventStoreMixin with all event-related operations.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from stabilize.events.base import (
    RAW_EVENT_TYPE,
    EntityType,
    Event,
    EventMetadata,
    EventType,
    parse_event_type,
)
from stabilize.events.store.interface import EventQuery
from stabilize.persistence.columns import select_list
from stabilize.persistence.tenancy import TenantMismatchError

_EVENTS = select_list("events", "sqlite")

if TYPE_CHECKING:
    pass


class SqliteEventStoreMixin:
    """Mixin providing event append, query, and retrieval methods."""

    tenant: str

    if TYPE_CHECKING:

        def _get_connection(self) -> sqlite3.Connection: ...

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
                if event.tenant and event.tenant != self.tenant:
                    raise TenantMismatchError(
                        f"event {event.event_id} carries tenant {event.tenant!r} but this store is {self.tenant!r}"
                    )
                cursor = conn.execute(
                    """
                    INSERT INTO events (
                        event_id, event_type, timestamp, entity_type, entity_id,
                        workflow_id, version, data, correlation_id, causation_id,
                        actor, source_handler, schema_version, stabilize_tenant
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        event.schema_version,
                        self.tenant,
                    ),
                )

                # Get the assigned sequence number
                sequence = cursor.lastrowid or 0
                result_events.append(replace(event.with_sequence(sequence), tenant=self.tenant))

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

        sql_parts = [f"SELECT {_EVENTS} FROM events WHERE stabilize_tenant = ?"]
        params: list[Any] = [self.tenant]

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
            f"""
            SELECT {_EVENTS} FROM events
            WHERE entity_type = ? AND entity_id = ? AND version > ? AND stabilize_tenant = ?
            ORDER BY sequence ASC
            """,
            (entity_type.value, entity_id, from_version, self.tenant),
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
            f"""
            SELECT {_EVENTS} FROM events
            WHERE workflow_id = ? AND sequence > ? AND stabilize_tenant = ?
            ORDER BY sequence ASC
            """,
            (workflow_id, from_sequence, self.tenant),
        )

        return [self._row_to_event(row) for row in cursor]

    def get_current_sequence(self) -> int:
        """Get the current (latest) global sequence number."""
        conn = self._get_connection()

        cursor = conn.execute("SELECT MAX(sequence) FROM events WHERE stabilize_tenant = ?", (self.tenant,))
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
            f"""
            SELECT {_EVENTS} FROM events
            WHERE sequence > ? AND stabilize_tenant = ?
            ORDER BY sequence ASC
            LIMIT ?
            """,
            (sequence, self.tenant, limit),
        )

        return [self._row_to_event(row) for row in cursor]

    def get_events_since_committed(
        self,
        cursor: str,
        limit: int = 1000,
        after_sequence: int = 0,
    ) -> tuple[list[Event], str, int]:
        """Events after the position, with the next position.

        SQLite serialises commits behind its write lock, so commit order is
        sequence order and the sequence cursor is already gap-free. This exists
        so subscription code can use one call on both backends; both parts of
        the position carry the sequence.
        """
        try:
            start = int(cursor or 0)
        except (TypeError, ValueError):
            start = 0
        start = max(start, int(after_sequence or 0))

        events = self.get_events_since(start, limit=limit)
        last = events[-1].sequence if events else start
        return events, str(last), last

    def supports_commit_cursor(self) -> bool:
        """SQLite commit order equals sequence order, so this is always safe."""
        return True

    def get_event_by_id(self, event_id: str) -> Event | None:
        """Get a single event by its ID."""
        conn = self._get_connection()

        cursor = conn.execute(
            f"SELECT {_EVENTS} FROM events WHERE event_id = ? AND stabilize_tenant = ?",
            (event_id, self.tenant),
        )

        row = cursor.fetchone()
        return self._row_to_event(row) if row else None

    def count_events(self, query: EventQuery | None = None) -> int:
        """Count events matching query."""
        conn = self._get_connection()

        if query is None:
            cursor = conn.execute("SELECT COUNT(*) FROM events WHERE stabilize_tenant = ?", (self.tenant,))
        else:
            sql_parts = ["SELECT COUNT(*) FROM events WHERE stabilize_tenant = ?"]
            params: list[Any] = [self.tenant]

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

        schema_ver = row["schema_version"] or 1

        parsed_type = parse_event_type(row["event_type"])
        if parsed_type is EventType.UNKNOWN:
            data = {**data, RAW_EVENT_TYPE: row["event_type"]}

        return Event(
            event_id=row["event_id"],
            event_type=parsed_type,
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
            schema_version=schema_ver,
            tenant=row["stabilize_tenant"] or "",
        )
