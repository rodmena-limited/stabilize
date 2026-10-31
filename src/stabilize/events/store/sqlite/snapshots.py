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
from stabilize.persistence.columns import select_list
from stabilize.persistence.tenancy import refuse_foreign_row

_SNAPSHOTS = select_list("snapshots", "sqlite")

if TYPE_CHECKING:
    pass


class SqliteSnapshotsMixin:
    """Mixin providing snapshot operations."""

    tenant: str

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

        state_json = json.dumps(state, default=str)
        state_hash = hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode()).hexdigest()
        cursor = conn.execute(
            """
            INSERT INTO snapshots
            (entity_type, entity_id, workflow_id, version, sequence, state, state_hash, stabilize_tenant)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (entity_type, entity_id, version) DO UPDATE SET
                sequence = excluded.sequence,
                state = excluded.state,
                state_hash = excluded.state_hash,
                created_at = CURRENT_TIMESTAMP
            WHERE snapshots.stabilize_tenant = excluded.stabilize_tenant
            """,
            (
                entity_type.value,
                entity_id,
                workflow_id,
                version,
                sequence,
                state_json,
                state_hash,
                self.tenant,
            ),
        )
        if cursor.rowcount == 0:
            held = conn.execute(
                "SELECT stabilize_tenant FROM snapshots WHERE entity_type = ? AND entity_id = ? AND version = ?",
                (entity_type.value, entity_id, version),
            ).fetchone()
            key = f"{entity_type.value}/{entity_id}/{version}"
            refuse_foreign_row("snapshots", key, held[0] if held else None, self.tenant)
        conn.commit()

    def get_latest_snapshot(
        self,
        entity_type: EntityType,
        entity_id: str,
    ) -> dict[str, Any] | None:
        """Get the latest snapshot for an entity."""
        conn = self._get_connection()

        cursor = conn.execute(
            f"""
            SELECT {_SNAPSHOTS} FROM snapshots
            WHERE entity_type = ? AND entity_id = ? AND stabilize_tenant = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            (entity_type.value, entity_id, self.tenant),
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
