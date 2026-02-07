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


def create_signals_table(conn: sqlite3.Connection) -> None:
    """Create the signals table if it doesn't exist."""
    for statement in SIGNALS_SCHEMA.split(";"):
        statement = statement.strip()
        if statement:
            conn.execute(statement)
    conn.commit()


def buffer_signal(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
    signal_name: str,
    signal_data: dict[str, Any] | None = None,
) -> int:
    """Buffer a signal for later consumption.

    Returns:
        The ID of the buffered signal.
    """
    cursor = conn.execute(
        """
        INSERT INTO workflow_signals (execution_id, stage_ref_id, signal_name, signal_data)
        VALUES (:execution_id, :stage_ref_id, :signal_name, :signal_data)
        """,
        {
            "execution_id": execution_id,
            "stage_ref_id": stage_ref_id,
            "signal_name": signal_name,
            "signal_data": json.dumps(signal_data or {}),
        },
    )
    return cursor.lastrowid or 0


def consume_signal(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
    signal_name: str | None = None,
) -> BufferedSignal | None:
    """Consume the oldest unconsumed signal for a stage.

    Args:
        conn: Database connection
        execution_id: Workflow execution ID
        stage_ref_id: Target stage ref_id
        signal_name: Optional signal name filter

    Returns:
        The consumed signal, or None if no signal available.
    """
    if signal_name:
        row = conn.execute(
            """
            SELECT id, execution_id, stage_ref_id, signal_name, signal_data, created_at
            FROM workflow_signals
            WHERE execution_id = :execution_id
              AND stage_ref_id = :stage_ref_id
              AND signal_name = :signal_name
              AND consumed = 0
            ORDER BY id ASC
            LIMIT 1
            """,
            {
                "execution_id": execution_id,
                "stage_ref_id": stage_ref_id,
                "signal_name": signal_name,
            },
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT id, execution_id, stage_ref_id, signal_name, signal_data, created_at
            FROM workflow_signals
            WHERE execution_id = :execution_id
              AND stage_ref_id = :stage_ref_id
              AND consumed = 0
            ORDER BY id ASC
            LIMIT 1
            """,
            {
                "execution_id": execution_id,
                "stage_ref_id": stage_ref_id,
            },
        ).fetchone()

    if row is None:
        return None

    # Mark as consumed
    now = datetime.now(UTC).isoformat()
    conn.execute(
        "UPDATE workflow_signals SET consumed = 1, consumed_at = :now WHERE id = :id",
        {"id": row["id"], "now": now},
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
    )


def get_pending_signals(
    conn: sqlite3.Connection,
    execution_id: str,
    stage_ref_id: str,
) -> list[BufferedSignal]:
    """Get all unconsumed signals for a stage.

    Returns:
        List of pending signals, oldest first.
    """
    rows = conn.execute(
        """
        SELECT id, execution_id, stage_ref_id, signal_name, signal_data, created_at
        FROM workflow_signals
        WHERE execution_id = :execution_id
          AND stage_ref_id = :stage_ref_id
          AND consumed = 0
        ORDER BY id ASC
        """,
        {
            "execution_id": execution_id,
            "stage_ref_id": stage_ref_id,
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
        )
        for row in rows
    ]


def cleanup_consumed_signals(
    conn: sqlite3.Connection,
    execution_id: str,
) -> int:
    """Remove consumed signals for an execution.

    Returns:
        Number of signals removed.
    """
    cursor = conn.execute(
        "DELETE FROM workflow_signals WHERE execution_id = :execution_id AND consumed = 1",
        {"execution_id": execution_id},
    )
    return cursor.rowcount
