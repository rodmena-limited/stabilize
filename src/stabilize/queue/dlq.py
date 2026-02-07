"""
Dead Letter Queue (DLQ) functionality for PostgreSQL queue.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def move_to_dlq(
    pool: Any,
    table_name: str,
    pending: dict[int, dict[str, Any]],
    message_id: int | str,
    error: str | None = None,
) -> None:
    """Move a message to the Dead Letter Queue.

    Args:
        pool: Database connection pool
        table_name: Name of the queue table
        pending: Pending messages dict (will be modified)
        message_id: The message ID (integer or string)
        error: Error message/details explaining why it was moved
    """
    msg_id = int(message_id)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Get the message from main queue
            cur.execute(
                f"""
                SELECT id, message_id, message_type, payload, attempts, created_at
                FROM {table_name}
                WHERE id = %(id)s
                """,
                {"id": msg_id},
            )
            row = cur.fetchone()

            if not row:
                logger.warning("Message %s not found for DLQ move", msg_id)
                return

            # Insert into DLQ
            cur.execute(
                f"""
                INSERT INTO {table_name}_dlq (
                    original_id, message_id, message_type, payload,
                    attempts, error, last_error_at, created_at
                ) VALUES (
                    %(original_id)s, %(message_id)s, %(message_type)s, %(payload)s,
                    %(attempts)s, %(error)s, NOW(), %(created_at)s
                )
                """,
                {
                    "original_id": row["id"],
                    "message_id": row["message_id"],
                    "message_type": row["message_type"],
                    "payload": (
                        json.dumps(row["payload"])
                        if isinstance(row["payload"], dict)
                        else row["payload"]
                    ),
                    "attempts": row["attempts"],
                    "error": error or "Max attempts exceeded",
                    "created_at": row["created_at"],
                },
            )

            # Delete from main queue
            cur.execute(
                f"DELETE FROM {table_name} WHERE id = %(id)s",
                {"id": msg_id},
            )
        conn.commit()

    pending.pop(msg_id, None)
    logger.warning(
        "Moved message to DLQ (id=%s, type=%s, attempts=%s, error=%s)",
        msg_id,
        row["message_type"],
        row["attempts"],
        error,
    )


def list_dlq(
    pool: Any,
    table_name: str,
    limit: int = 100,
    message_type: str | None = None,
) -> list[dict[str, Any]]:
    """List messages in the Dead Letter Queue.

    Args:
        pool: Database connection pool
        table_name: Name of the queue table
        limit: Maximum number of messages to return
        message_type: Optional filter by message type

    Returns:
        List of DLQ message dictionaries
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            if message_type:
                cur.execute(
                    f"""
                    SELECT * FROM {table_name}_dlq
                    WHERE message_type = %(message_type)s
                    ORDER BY moved_at DESC
                    LIMIT %(limit)s
                    """,
                    {"message_type": message_type, "limit": limit},
                )
            else:
                cur.execute(
                    f"""
                    SELECT * FROM {table_name}_dlq
                    ORDER BY moved_at DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": limit},
                )
            rows = cur.fetchall()
            return [dict(row) for row in rows]


def replay_dlq(pool: Any, table_name: str, dlq_id: int) -> bool:
    """Replay a message from the DLQ back to the main queue.

    Args:
        pool: Database connection pool
        table_name: Name of the queue table
        dlq_id: The DLQ entry ID

    Returns:
        True if replayed successfully, False if not found
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Get the DLQ entry
            cur.execute(
                f"SELECT * FROM {table_name}_dlq WHERE id = %(id)s",
                {"id": dlq_id},
            )
            row = cur.fetchone()

            if not row:
                logger.warning("DLQ entry %s not found for replay", dlq_id)
                return False

            # Insert back into main queue with reset attempts
            cur.execute(
                f"""
                INSERT INTO {table_name} (
                    message_id, message_type, payload, deliver_at, attempts
                ) VALUES (
                    %(message_id)s, %(message_type)s, %(payload)s::jsonb, NOW(), 0
                )
                """,
                {
                    "message_id": row["message_id"] + "-replay",
                    "message_type": row["message_type"],
                    "payload": (
                        json.dumps(row["payload"])
                        if isinstance(row["payload"], dict)
                        else row["payload"]
                    ),
                },
            )

            # Delete from DLQ
            cur.execute(
                f"DELETE FROM {table_name}_dlq WHERE id = %(id)s",
                {"id": dlq_id},
            )
        conn.commit()

    logger.info("Replayed DLQ entry %s (type=%s)", dlq_id, row["message_type"])
    return True


def dlq_size(pool: Any, table_name: str) -> int:
    """Get the number of messages in the Dead Letter Queue."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {table_name}_dlq")
            row = cur.fetchone()
            return row["cnt"] if row else 0


def clear_dlq(pool: Any, table_name: str) -> int:
    """Clear all messages from the Dead Letter Queue.

    Returns:
        Number of messages cleared
    """
    count = dlq_size(pool, table_name)
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name}_dlq")
        conn.commit()
    logger.info("Cleared %d messages from DLQ", count)
    return count


def check_and_move_expired(
    pool: Any,
    table_name: str,
    pending: dict[int, dict[str, Any]],
    max_attempts: int,
) -> int:
    """Check for messages exceeding max_attempts and move to DLQ.

    Returns:
        Number of messages moved to DLQ
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Find messages that have exceeded max_attempts
            cur.execute(
                f"""
                SELECT id, message_type, attempts
                FROM {table_name}
                WHERE attempts >= %(max_attempts)s
                """,
                {"max_attempts": max_attempts},
            )
            rows = cur.fetchall()

    moved_count = 0
    for row in rows:
        move_to_dlq(
            pool,
            table_name,
            pending,
            row["id"],
            f"Exceeded max_attempts ({row['attempts']})",
        )
        moved_count += 1

    if moved_count > 0:
        logger.info("Moved %d expired messages to DLQ", moved_count)

    return moved_count
