"""
Signal persistence for WCP-24 (Persistent Trigger).

Provides a table for buffering signals that arrive before the target
stage is ready to consume them.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

SIGNALS_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    stage_ref_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    signal_data TEXT DEFAULT '{}',
    consumed INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now', 'utc')),
    consumed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_signals_stage
    ON workflow_signals(execution_id, stage_ref_id, consumed);
CREATE INDEX IF NOT EXISTS idx_signals_name
    ON workflow_signals(signal_name);
"""


@dataclass
class BufferedSignal:
    """A buffered signal waiting to be consumed."""

    id: int = 0
    execution_id: str = ""
    stage_ref_id: str = ""
    signal_name: str = ""
    signal_data: dict[str, Any] = field(default_factory=dict)
    consumed: bool = False
    created_at: str = ""
    consumed_at: str | None = None
    user: str = ""
    persistent: bool = True


def signal_table_statements() -> tuple[str, ...]:
    """The baseline DDL of workflow_signals, one statement per element."""
    return tuple(statement.strip() for statement in SIGNALS_SCHEMA.split(";") if statement.strip())


def create_signals_table(conn: sqlite3.Connection) -> None:
    """Create the signals table if it doesn't exist."""
    for statement in signal_table_statements():
        conn.execute(statement)
    conn.commit()
    from stabilize.persistence.sqlite.tenant_schema import ensure_tenant_schema

    ensure_tenant_schema(conn)


def buffer_signal(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
    signal_name: str,
    signal_data: dict[str, Any] | None = None,
    tenant: str = "",
    *,
    consumed: bool = False,
    user: str = "",
    persistent: bool = True,
) -> int:
    """Store a signal row; consumed=True records a signal delivered straight into the stage context.

    Returns:
        The ID of the stored signal.
    """
    cursor = conn.execute(
        """
        INSERT INTO workflow_signals (execution_id, stage_ref_id, signal_name, signal_data, consumed, consumed_at,
                                      signal_user, persistent, stabilize_tenant)
        VALUES (:execution_id, :stage_ref_id, :signal_name, :signal_data, :consumed, :consumed_at,
                :user, :persistent, :tenant)
        """,
        {
            "execution_id": execution_id,
            "stage_ref_id": stage_ref_id,
            "signal_name": signal_name,
            "signal_data": json.dumps(signal_data or {}, default=str),
            "consumed": 1 if consumed else 0,
            "consumed_at": datetime.now(UTC).isoformat() if consumed else None,
            "user": user,
            "persistent": 1 if persistent else 0,
            "tenant": tenant,
        },
    )
    return cursor.lastrowid or 0


def consume_signal(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
    signal_name: str | None = None,
    tenant: str = "",
    *,
    row_id: int | None = None,
) -> BufferedSignal | None:
    """Consume the oldest unconsumed signal for a stage, or the one row named by row_id.

    Returns:
        The consumed signal, or None if no signal available.
    """
    clauses = ""
    params: dict[str, Any] = {"execution_id": execution_id, "stage_ref_id": stage_ref_id, "tenant": tenant}
    if signal_name:
        clauses += " AND signal_name = :signal_name"
        params["signal_name"] = signal_name
    if row_id is not None:
        clauses += " AND id = :row_id"
        params["row_id"] = row_id
    row = conn.execute(
        "SELECT id, execution_id, stage_ref_id, signal_name, signal_data, created_at, signal_user, persistent "
        "FROM workflow_signals WHERE execution_id = :execution_id AND stage_ref_id = :stage_ref_id "
        f"AND consumed = 0 AND stabilize_tenant = :tenant{clauses} ORDER BY id ASC LIMIT 1",
        params,
    ).fetchone()
    if row is None:
        return None

    now = datetime.now(UTC).isoformat()
    conn.execute(
        "UPDATE workflow_signals SET consumed = 1, consumed_at = :now WHERE id = :id AND stabilize_tenant = :tenant",
        {"id": row["id"], "now": now, "tenant": tenant},
    )
    return BufferedSignal(
        id=row["id"],
        execution_id=row["execution_id"],
        stage_ref_id=row["stage_ref_id"],
        signal_name=row["signal_name"],
        signal_data=json.loads(row["signal_data"] or "{}"),
        consumed=True,
        created_at=row["created_at"],
        consumed_at=now,
        user=row["signal_user"] or "",
        persistent=bool(row["persistent"]),
    )


def get_pending_signals(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
    tenant: str = "",
) -> list[BufferedSignal]:
    """Get all unconsumed signals for a stage.

    Returns:
        List of pending signals, oldest first.
    """
    rows = conn.execute(
        """
        SELECT id, execution_id, stage_ref_id, signal_name, signal_data, created_at, signal_user, persistent
        FROM workflow_signals
        WHERE execution_id = :execution_id
          AND stage_ref_id = :stage_ref_id
          AND consumed = 0 AND stabilize_tenant = :tenant
        ORDER BY id ASC
        """,
        {
            "execution_id": execution_id,
            "stage_ref_id": stage_ref_id,
            "tenant": tenant,
        },
    ).fetchall()

    return [
        BufferedSignal(
            id=row["id"],
            execution_id=row["execution_id"],
            stage_ref_id=row["stage_ref_id"],
            signal_name=row["signal_name"],
            signal_data=json.loads(row["signal_data"] or "{}"),
            created_at=row["created_at"],
            user=row["signal_user"] or "",
            persistent=bool(row["persistent"]),
        )
        for row in rows
    ]


def cleanup_consumed_signals(
    conn: sqlite3.Connection,
    execution_id: str,
    tenant: str = "",
) -> int:
    """Remove consumed signals for an execution.

    Returns:
        Number of signals removed.
    """
    cursor = conn.execute(
        "DELETE FROM workflow_signals "
        "WHERE execution_id = :execution_id AND consumed = 1 AND stabilize_tenant = :tenant",
        {"execution_id": execution_id, "tenant": tenant},
    )
    return cursor.rowcount
