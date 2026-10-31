"""
Dead Letter Queue (DLQ) functionality for PostgreSQL queue.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from ulid import ULID

from stabilize.persistence.columns import select_list
from stabilize.queue.message_ids import row_id

_DLQ = select_list("queue_messages_dlq", "postgres")

logger = logging.getLogger(__name__)


def move_to_dlq(
    engine: Any,
    table_name: str,
    pending: dict[int, dict[str, Any]],
    message_id: int | str,
    error: str | None = None,
) -> None:
    """Move a message to the Dead Letter Queue.

    The row is copied server-side, so its payload reaches the DLQ exactly as
    stored, whatever JSON it holds.

    Args:
        pool: Database connection pool
        table_name: Name of the queue table
        pending: Pending messages dict (will be modified)
        message_id: The message ID (integer or string)
        error: Error message/details explaining why it was moved
    """
    msg_id = row_id(message_id)

    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH moved AS (
                    DELETE FROM {table_name}
                    WHERE id = %(id)s AND stabilize_tenant = %(tenant)s
                    RETURNING id, message_id, message_type, payload, attempts, created_at, stabilize_tenant,
                              store_retries, store_retry_since
                )
                INSERT INTO {table_name}_dlq (
                    original_id, message_id, message_type, payload,
                    attempts, error, last_error_at, created_at, stabilize_tenant,
                    store_retries, store_retry_since
                )
                SELECT id, message_id, message_type, payload,
                       attempts, %(error)s, NOW(), created_at, stabilize_tenant,
                       store_retries, store_retry_since
                FROM moved
                RETURNING message_type, attempts
                """,
                {"id": msg_id, "error": error or "Max attempts exceeded", "tenant": engine.tenant},
            )
            row = cur.fetchone()
        conn.commit()

    if not row:
        logger.warning("Message %s not found for DLQ move", msg_id)
        return

    pending.pop(msg_id, None)
    logger.warning(
        "Moved message to DLQ (id=%s, type=%s, attempts=%s, error=%s)",
        msg_id,
        row["message_type"],
        row["attempts"],
        error,
    )


def list_dlq(
    engine: Any,
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
    with engine.connection() as conn:
        with conn.cursor() as cur:
            if message_type:
                cur.execute(
                    f"""
                    SELECT {_DLQ} FROM {table_name}_dlq
                    WHERE message_type = %(message_type)s AND stabilize_tenant = %(tenant)s
                    ORDER BY moved_at DESC
                    LIMIT %(limit)s
                    """,
                    {"message_type": message_type, "limit": limit, "tenant": engine.tenant},
                )
            else:
                cur.execute(
                    f"""
                    SELECT {_DLQ} FROM {table_name}_dlq
                    WHERE stabilize_tenant = %(tenant)s
                    ORDER BY moved_at DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": limit, "tenant": engine.tenant},
                )
            rows = cur.fetchall()
            return [dict(row) for row in rows]


def replay_dlq(engine: Any, table_name: str, dlq_id: int) -> bool:
    """Replay a message from the DLQ back to the main queue.

    Args:
        pool: Database connection pool
        table_name: Name of the queue table
        dlq_id: The DLQ entry ID

    Returns:
        True if replayed successfully, False if not found
    """
    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH taken AS (
                    DELETE FROM {table_name}_dlq
                    WHERE id = %(id)s AND stabilize_tenant = %(tenant)s
                    RETURNING message_type, payload, stabilize_tenant
                )
                INSERT INTO {table_name} (
                    message_id, message_type, payload, deliver_at, attempts, stabilize_tenant
                )
                SELECT %(message_id)s, message_type, payload, NOW(), 0, stabilize_tenant
                FROM taken
                RETURNING message_type
                """,
                {"id": dlq_id, "message_id": str(ULID()), "tenant": engine.tenant},
            )
            row = cur.fetchone()
        conn.commit()

    if not row:
        logger.warning("DLQ entry %s not found for replay", dlq_id)
        return False

    logger.info("Replayed DLQ entry %s (type=%s)", dlq_id, row["message_type"])
    return True


def dlq_size(engine: Any, table_name: str) -> int:
    """Get the number of messages in the Dead Letter Queue."""
    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) as cnt FROM {table_name}_dlq WHERE stabilize_tenant = %(tenant)s",
                {"tenant": engine.tenant},
            )
            row = cur.fetchone()
            return row["cnt"] if row else 0


def clear_dlq(engine: Any, table_name: str) -> int:
    """Clear all messages from the Dead Letter Queue.

    Returns:
        Number of messages cleared
    """
    count = dlq_size(engine, table_name)
    with engine.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name}_dlq WHERE stabilize_tenant = %(tenant)s", {"tenant": engine.tenant})
        conn.commit()
    logger.info("Cleared %d messages from DLQ", count)
    return count


def dead_letter_cause(row: Any) -> str:
    """Why the sweep moves a row: the store budget ran out, or the attempts did."""
    if row["over_budget"]:
        return f"store_unavailable_budget_exhausted (attempts {row['attempts']}, store retries {row['store_retries']})"
    return f"Exceeded max_attempts ({row['attempts']})"


def check_and_move_expired(
    engine: Any,
    table_name: str,
    pending: dict[int, dict[str, Any]],
    max_attempts: int,
    store_retry_budget: timedelta = timedelta(minutes=30),
) -> int:
    """Move to the DLQ what exceeded max_attempts once its store-retry budget, if any, is spent.

    Returns:
        Number of messages moved to DLQ
    """
    with engine.connection() as conn:
        with conn.cursor() as cur:
            # Find messages that have exceeded max_attempts
            cur.execute(
                f"""
                SELECT id, message_type, attempts, store_retries,
                       store_retry_since IS NOT NULL AS over_budget
                FROM {table_name}
                WHERE stabilize_tenant = %(tenant)s
                AND (locked_until IS NULL OR locked_until < NOW())
                AND attempts >= %(max_attempts)s
                AND (store_retry_since IS NULL
                     OR store_retry_since <= NOW() - %(budget)s * INTERVAL '1 second')
                """,
                {"max_attempts": max_attempts, "tenant": engine.tenant, "budget": store_retry_budget.total_seconds()},
            )
            rows = cur.fetchall()

    moved_count = 0
    for row in rows:
        move_to_dlq(
            engine,
            table_name,
            pending,
            row["id"],
            dead_letter_cause(row),
        )
        moved_count += 1

    if moved_count > 0:
        logger.info("Moved %d expired messages to DLQ", moved_count)

    return moved_count
