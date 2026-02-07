"""
Subscription CRUD operations for PostgreSQL event store.
"""

from __future__ import annotations

import json
from typing import Any

from stabilize.events.base import EventType


class PostgresSubscriptionsMixin:
    """Mixin providing durable subscription CRUD methods."""

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
