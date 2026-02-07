"""
Snapshot operations for SQLite event store.

Provides the SqliteSnapshotsMixin with snapshot save/retrieve methods.
"""

from __future__ import annotations

import json
from typing import Any

from stabilize.events.base import EntityType


class SqliteSnapshotsMixin:
    """Mixin providing snapshot operations."""

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
