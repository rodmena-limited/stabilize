"""
Subscription CRUD operations for SQLite event store.

Provides the SqliteSubscriptionsMixin with subscription management methods.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING, Any

from stabilize.events.base import EventType

if TYPE_CHECKING:
    pass


class SqliteSubscriptionsMixin:
    """Mixin providing subscription CRUD operations."""

    if TYPE_CHECKING:

        def _get_connection(self) -> sqlite3.Connection: ...

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
