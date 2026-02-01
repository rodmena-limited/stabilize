"""Extended operations for PostgreSQL persistence (pause, resume, cancel, deduplication)."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import PausedDetails
from stabilize.persistence.postgres.converters import paused_to_dict

logger = logging.getLogger(__name__)


def pause_execution(pool: Any, execution_id: str, paused_by: str) -> None:
    """Pause an execution."""
    paused = PausedDetails(
        paused_by=paused_by,
        pause_time=int(time.time() * 1000),
    )

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_executions SET
                    status = %(status)s,
                    paused = %(paused)s::jsonb
                WHERE id = %(id)s
                """,
                {
                    "id": execution_id,
                    "status": WorkflowStatus.PAUSED.name,
                    "paused": json.dumps(paused_to_dict(paused)),
                },
            )
        conn.commit()


def resume_execution(pool: Any, execution_id: str) -> None:
    """Resume a paused execution.

    Uses atomic UPDATE with status check to prevent race conditions.
    """
    current_time = int(time.time() * 1000)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_executions SET
                    status = %(status)s,
                    paused = CASE
                        WHEN paused IS NOT NULL THEN
                            jsonb_set(
                                jsonb_set(paused, '{resume_time}', to_jsonb(%(resume_time)s)),
                                '{paused_ms}',
                                to_jsonb(%(resume_time)s - (paused->>'pause_time')::bigint)
                            )
                        ELSE NULL
                    END
                WHERE id = %(id)s AND status = 'PAUSED'
                """,
                {
                    "id": execution_id,
                    "status": WorkflowStatus.RUNNING.name,
                    "resume_time": current_time,
                },
            )
            if cur.rowcount == 0:
                logger.warning(
                    "Resume had no effect for execution %s - not in PAUSED status",
                    execution_id,
                )
        conn.commit()


def cancel_execution(pool: Any, execution_id: str, canceled_by: str, reason: str) -> None:
    """Cancel an execution."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_executions SET
                    is_canceled = TRUE,
                    canceled_by = %(canceled_by)s,
                    cancellation_reason = %(reason)s
                WHERE id = %(id)s
                """,
                {
                    "id": execution_id,
                    "canceled_by": canceled_by,
                    "reason": reason,
                },
            )
        conn.commit()


def is_message_processed(pool: Any, message_id: str) -> bool:
    """Check if a message has already been processed."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM processed_messages WHERE message_id = %(message_id)s",
                {"message_id": message_id},
            )
            return cur.fetchone() is not None


def mark_message_processed(
    pool: Any,
    message_id: str,
    handler_type: str | None = None,
    execution_id: str | None = None,
) -> None:
    """Mark a message as successfully processed."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_messages (
                    message_id, processed_at, handler_type, execution_id
                ) VALUES (
                    %(message_id)s, NOW(), %(handler_type)s, %(execution_id)s
                )
                ON CONFLICT (message_id) DO NOTHING
                """,
                {
                    "message_id": message_id,
                    "handler_type": handler_type,
                    "execution_id": execution_id,
                },
            )
        conn.commit()


def cleanup_old_processed_messages(pool: Any, max_age_hours: float = 24.0) -> int:
    """Clean up old processed message records."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM processed_messages
                WHERE processed_at < NOW() - make_interval(hours => %(hours)s)
                """,
                {"hours": max_age_hours},
            )
            deleted: int = cur.rowcount or 0
        conn.commit()
        return deleted
