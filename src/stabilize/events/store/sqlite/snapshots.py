"""
Snapshot operations for SQLite event store.

Provides the SqliteSnapshotsMixin with snapshot save/retrieve methods.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import TYPE_CHECKING, Any

from stabilize.events.base import EntityType

if TYPE_CHECKING:
    pass


class SqliteSnapshotsMixin:
    """Mixin providing snapshot operations."""

    if TYPE_CHECKING:

        def _get_connection(self) -> sqlite3.Connection: ...

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

        state_json = json.dumps(state)
        state_hash = hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode()).hexdigest()
        conn.execute(
            """
            INSERT OR REPLACE INTO snapshots
            (entity_type, entity_id, workflow_id, version, sequence, state, state_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_type.value,
                entity_id,
                workflow_id,
                version,
                sequence,
                state_json,
                state_hash,
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

        result = {
            "entity_type": row["entity_type"],
            "entity_id": row["entity_id"],
            "workflow_id": row["workflow_id"],
            "version": row["version"],
            "sequence": row["sequence"],
            "state": state,
        }
        state_hash = row["state_hash"] if "state_hash" in row.keys() else None
        if state_hash is not None:
            result["state_hash"] = state_hash
        return result
