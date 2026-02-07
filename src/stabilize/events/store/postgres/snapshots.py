"""
Snapshot operations for PostgreSQL event store.
"""

from __future__ import annotations

import json
from typing import Any

from stabilize.events.base import EntityType


class PostgresSnapshotsMixin:
    """Mixin providing snapshot save and retrieval methods."""

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
