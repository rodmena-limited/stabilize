"""
Event append, query, and retrieval methods for PostgreSQL event store.
"""

from __future__ import annotations

import json
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
from stabilize.persistence.columns import columns, select_list
from stabilize.persistence.tenancy import TenantMismatchError

_EVENTS = select_list("events", "postgres")

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import DictRow
    from psycopg_pool import ConnectionPool


class PostgresEventsMixin:
    tenant: str
    _borrow: Any
    """Mixin providing event append, query, and retrieval methods."""

    if TYPE_CHECKING:
        _pool: ConnectionPool[Connection[DictRow]]

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
                if event.tenant and event.tenant != self.tenant:
                    raise TenantMismatchError(
                        f"event {event.event_id} carries tenant {event.tenant!r} but this store is {self.tenant!r}"
                    )
                cur.execute(
                    """
                    INSERT INTO events (
                        event_id, event_type, timestamp, entity_type, entity_id,
                        workflow_id, version, data, correlation_id, causation_id,
                        actor, source_handler, schema_version, stabilize_tenant
                    ) VALUES (
                        %(event_id)s, %(event_type)s, %(timestamp)s, %(entity_type)s,
                        %(entity_id)s, %(workflow_id)s, %(version)s, %(data)s,
                        %(correlation_id)s, %(causation_id)s, %(actor)s, %(source_handler)s,
                        %(schema_version)s, %(tenant)s
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
                        "tenant": self.tenant,
                        "data": json.dumps(event.data),
                        "correlation_id": event.metadata.correlation_id,
                        "causation_id": event.metadata.causation_id,
                        "actor": event.metadata.actor,
                        "source_handler": event.metadata.source_handler,
                        "schema_version": event.schema_version,
                    },
                )

                row = cur.fetchone()
                sequence = row[0] if isinstance(row, tuple) else row.get("sequence", 0)
                result_events.append(replace(event.with_sequence(sequence), tenant=self.tenant))

            return result_events

        if connection is not None:
            with connection.cursor() as cur:
                return _do_append(cur)
        else:
            with self._borrow() as conn:
                with conn.cursor() as cur:
                    result = _do_append(cur)
                conn.commit()
                return result

    def get_events(self, query: EventQuery) -> Iterator[Event]:
        """Query events matching criteria."""
        sql_parts = [f"SELECT {_EVENTS} FROM events WHERE stabilize_tenant = %(tenant)s"]
        params: dict[str, Any] = {"tenant": self.tenant}

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

        sql: Any = " ".join(sql_parts)

        with self._borrow() as conn:
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
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {_EVENTS} FROM events
                    WHERE entity_type = %(entity_type)s
                      AND entity_id = %(entity_id)s
                      AND version > %(from_version)s AND stabilize_tenant = %(tenant)s
                    ORDER BY sequence ASC
                    """,
                    {
                        "entity_type": entity_type.value,
                        "entity_id": entity_id,
                        "from_version": from_version,
                        "tenant": self.tenant,
                    },
                )
                return [self._row_to_event(row) for row in cur.fetchall()]

    def get_events_for_workflow(
        self,
        workflow_id: str,
        from_sequence: int = 0,
    ) -> list[Event]:
        """Get all events for a workflow execution."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {_EVENTS} FROM events
                    WHERE workflow_id = %(workflow_id)s
                      AND sequence > %(from_sequence)s AND stabilize_tenant = %(tenant)s
                    ORDER BY sequence ASC
                    """,
                    {"workflow_id": workflow_id, "from_sequence": from_sequence, "tenant": self.tenant},
                )
                return [self._row_to_event(row) for row in cur.fetchall()]

    def get_current_sequence(self) -> int:
        """Get the current (latest) global sequence number."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(sequence) FROM events WHERE stabilize_tenant = %(tenant)s", {"tenant": self.tenant}
                )
                row = cur.fetchone()
                if row is None:
                    return 0
                value = row.get("max")
                return value if value is not None else 0

    def get_events_since(
        self,
        sequence: int,
        limit: int = 1000,
    ) -> list[Event]:
        """Get events since a sequence number."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {_EVENTS} FROM events
                    WHERE sequence > %(sequence)s AND stabilize_tenant = %(tenant)s
                    ORDER BY sequence ASC
                    LIMIT %(limit)s
                    """,
                    {"sequence": sequence, "limit": limit, "tenant": self.tenant},
                )
                return [self._row_to_event(row) for row in cur.fetchall()]

    def get_events_since_committed(
        self,
        cursor: str,
        limit: int = 1000,
        after_sequence: int = 0,
    ) -> tuple[list[Event], str, int]:
        """Committed events after the position `(cursor, after_sequence)` in commit order, with the next position.

        `sequence` is assigned at INSERT and not at COMMIT, so a cursor that
        advances by sequence can step over an event whose transaction had not
        committed yet, losing it permanently. pg_snapshot_xmin() is the lowest
        transaction id still in progress, so nothing below it can commit later:
        it is a monotone, gap-free frontier.

        A full batch leaves the position on its last event, so the next call
        continues with the events that did not fit; a short batch moves it to
        the watermark. Events written before the commit_xid column exists carry
        NULL and are coalesced to '0'::xid8, which keeps them deliverable.
        """
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_snapshot_xmin(pg_current_snapshot())::text AS wm")
                row = cur.fetchone()
                if row is None:
                    return [], cursor or "0", int(after_sequence or 0)
                watermark = str(row["wm"] if isinstance(row, dict) else row[0])

                cur.execute(
                    f"""
                    SELECT {_EVENTS} FROM events
                    WHERE stabilize_tenant = %(tenant)s
                      AND (coalesce(commit_xid, '0'::xid8), sequence) > (%(cursor)s::xid8, %(after)s::bigint)
                      AND coalesce(commit_xid, '0'::xid8) < %(watermark)s::xid8
                    ORDER BY coalesce(commit_xid, '0'::xid8) ASC, sequence ASC
                    LIMIT %(limit)s
                    """,
                    {
                        "cursor": cursor or "0",
                        "after": int(after_sequence or 0),
                        "watermark": watermark,
                        "limit": limit,
                        "tenant": self.tenant,
                    },
                )
                rows = cur.fetchall()
                events = [self._row_to_event(row) for row in rows]

        if rows and len(rows) >= limit:
            last = rows[-1]
            last_xid = last["commit_xid"] if isinstance(last, dict) else None
            return events, str(last_xid if last_xid is not None else "0"), events[-1].sequence
        return events, watermark, 0

    def get_event_by_id(self, event_id: str) -> Event | None:
        """Get a single event by its ID."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {_EVENTS} FROM events WHERE event_id = %(event_id)s AND stabilize_tenant = %(tenant)s",
                    {"event_id": event_id, "tenant": self.tenant},
                )
                row = cur.fetchone()
                return self._row_to_event(row) if row else None

    def count_events(self, query: EventQuery | None = None) -> int:
        """Count events matching query."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                if query is None:
                    cur.execute(
                        "SELECT COUNT(*) FROM events WHERE stabilize_tenant = %(tenant)s", {"tenant": self.tenant}
                    )
                else:
                    sql_parts = ["SELECT COUNT(*) FROM events WHERE stabilize_tenant = %(tenant)s"]
                    params: dict[str, Any] = {"tenant": self.tenant}

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

                    sql_query: Any = " ".join(sql_parts)
                    cur.execute(sql_query, params)

                row = cur.fetchone()
                if row is None:
                    return 0
                count_val = row.get("count", 0)
                return count_val if count_val is not None else 0

    def _row_to_event(self, row: Any) -> Event:
        """Convert a database row to an Event."""
        data = row if isinstance(row, dict) else dict(zip(columns("events", "postgres"), row, strict=True))

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

        parsed_type = parse_event_type(data["event_type"])
        if parsed_type is EventType.UNKNOWN:
            event_data = {**event_data, RAW_EVENT_TYPE: data["event_type"]}

        return Event(
            event_id=data["event_id"],
            event_type=parsed_type,
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
            schema_version=data.get("schema_version", 1),
            tenant=data["stabilize_tenant"] or "",
        )
