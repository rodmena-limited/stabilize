"""
Snapshot operations for PostgreSQL event store.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from stabilize.events.base import EntityType
from stabilize.persistence.columns import select_list
from stabilize.persistence.tenancy import refuse_foreign_row

_SNAPSHOTS = select_list("snapshots", "postgres")

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import DictRow
    from psycopg_pool import ConnectionPool


class PostgresSnapshotsMixin:
    tenant: str
    """Mixin providing snapshot save and retrieval methods."""

    if TYPE_CHECKING:
        _pool: ConnectionPool[Connection[DictRow]]

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
                state_json = json.dumps(state, default=str)
                state_hash = hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode()).hexdigest()
                cur.execute(
                    """
                    INSERT INTO snapshots
                    (entity_type, entity_id, workflow_id, version, sequence, state, state_hash, stabilize_tenant)
                    VALUES (%(entity_type)s, %(entity_id)s, %(workflow_id)s,
                            %(version)s, %(sequence)s, %(state)s, %(state_hash)s, %(tenant)s)
                    ON CONFLICT (entity_type, entity_id, version)
                    DO UPDATE SET
                        sequence = EXCLUDED.sequence,
                        state = EXCLUDED.state,
                        state_hash = EXCLUDED.state_hash,
                        created_at = NOW()
                    WHERE snapshots.stabilize_tenant = EXCLUDED.stabilize_tenant
                    """,
                    {
                        "entity_type": entity_type.value,
                        "entity_id": entity_id,
                        "workflow_id": workflow_id,
                        "version": version,
                        "sequence": sequence,
                        "state": state_json,
                        "state_hash": state_hash,
                        "tenant": self.tenant,
                    },
                )
                if cur.rowcount == 0:
                    cur.execute(
                        "SELECT stabilize_tenant AS owner FROM snapshots "
                        "WHERE entity_type = %(entity_type)s AND entity_id = %(entity_id)s AND version = %(version)s",
                        {"entity_type": entity_type.value, "entity_id": entity_id, "version": version},
                    )
                    held = cur.fetchone()
                    refuse_foreign_row(
                        "snapshots",
                        f"{entity_type.value}/{entity_id}/{version}",
                        held["owner"] if held else None,
                        self.tenant,
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
                    f"""
                    SELECT {_SNAPSHOTS} FROM snapshots
                    WHERE entity_type = %(entity_type)s AND entity_id = %(entity_id)s AND stabilize_tenant = %(tenant)s
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                    {"entity_type": entity_type.value, "entity_id": entity_id, "tenant": self.tenant},
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
                        "state_hash": row[7] if len(row) > 7 else None,
                    }

                state = data["state"]
                if isinstance(state, str):
                    try:
                        state = json.loads(state)
                    except (json.JSONDecodeError, TypeError):
                        state = {}

                result = {
                    "entity_type": data["entity_type"],
                    "entity_id": data["entity_id"],
                    "workflow_id": data["workflow_id"],
                    "version": data["version"],
                    "sequence": data["sequence"],
                    "state": state,
                }
                state_hash = data.get("state_hash")
                if state_hash is not None:
                    result["state_hash"] = state_hash
                return result
