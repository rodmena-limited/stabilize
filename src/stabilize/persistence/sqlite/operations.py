"""Extended operations for SQLite persistence (pause, resume, cancel, deduplication)."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import UTC, datetime, timedelta

from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import PausedDetails
from stabilize.persistence.sqlite.converters import paused_to_dict

logger = logging.getLogger(__name__)


def pause_execution(
    conn: sqlite3.Connection,
    execution_id: str,
    paused_by: str,
) -> None:
    """Pause an execution."""
    paused = PausedDetails(
        paused_by=paused_by,
        pause_time=int(time.time() * 1000),
    )

    conn.execute(
        """
        UPDATE pipeline_executions SET
            status = :status,
            paused = :paused
        WHERE id = :id
        """,
        {
            "id": execution_id,
            "status": WorkflowStatus.PAUSED.name,
            "paused": json.dumps(paused_to_dict(paused)),
        },
    )
    conn.commit()


def resume_execution(
    conn: sqlite3.Connection,
    execution_id: str,
) -> None:
    """Resume a paused execution.

    Uses atomic UPDATE with status check to prevent race conditions.
    Calculates resume_time and paused_ms atomically in the UPDATE to avoid
    race conditions between retrieve and update.
    """
    current_time = int(time.time() * 1000)

    # Atomic update: calculate resume_time and paused_ms in the UPDATE itself
    # This prevents race conditions where paused details could change between read and write
    cursor = conn.execute(
        """
        UPDATE pipeline_executions SET
            status = :status,
            paused = CASE
                WHEN paused IS NOT NULL THEN
                    json_set(
                        json_set(paused, '$.resume_time', :resume_time),
                        '$.paused_ms',
                        :resume_time - CAST(json_extract(paused, '$.pause_time') AS INTEGER)
                    )
                ELSE NULL
            END
        WHERE id = :id AND status = 'PAUSED'
        """,
        {
            "id": execution_id,
            "status": WorkflowStatus.RUNNING.name,
            "resume_time": current_time,
        },
    )
    conn.commit()
    if cursor.rowcount == 0:
        logger.warning(
            "Resume had no effect for execution %s - not in PAUSED status",
            execution_id,
        )


def cancel_execution(
    conn: sqlite3.Connection,
    execution_id: str,
    canceled_by: str,
    reason: str,
) -> None:
    """Cancel an execution."""
    conn.execute(
        """
        UPDATE pipeline_executions SET
            is_canceled = 1,
            canceled_by = :canceled_by,
            cancellation_reason = :reason
        WHERE id = :id
        """,
        {
            "id": execution_id,
            "canceled_by": canceled_by,
            "reason": reason,
        },
    )
    conn.commit()


def is_message_processed(
    conn: sqlite3.Connection,
    message_id: str,
) -> bool:
    """Check if a message has already been processed."""
    result = conn.execute(
        "SELECT 1 FROM processed_messages WHERE message_id = :message_id",
        {"message_id": message_id},
    ).fetchone()
    return result is not None


def mark_message_processed(
    conn: sqlite3.Connection,
    message_id: str,
    handler_type: str | None = None,
    execution_id: str | None = None,
) -> None:
    """Mark a message as successfully processed."""
    conn.execute(
        """
        INSERT OR IGNORE INTO processed_messages (
            message_id, processed_at, handler_type, execution_id
        ) VALUES (
            :message_id, datetime('now', 'utc'), :handler_type, :execution_id
        )
        """,
        {
            "message_id": message_id,
            "handler_type": handler_type,
            "execution_id": execution_id,
        },
    )
    conn.commit()


def cleanup_old_processed_messages(
    conn: sqlite3.Connection,
    max_age_hours: float = 24.0,
) -> int:
    """Clean up old processed message records."""
    cutoff = datetime.now(UTC) - timedelta(hours=max_age_hours)
    cursor = conn.execute(
        "DELETE FROM processed_messages WHERE processed_at < :cutoff",
        {"cutoff": cutoff.isoformat()},
    )
    conn.commit()
    return cursor.rowcount
