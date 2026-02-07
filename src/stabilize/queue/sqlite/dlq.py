"""Dead Letter Queue mixin for SQLite queue."""

from __future__ import annotations

import logging
import sqlite3
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class SqliteDLQMixin:
    """Mixin providing Dead Letter Queue functionality for SQLite queue.

    This mixin provides methods for managing messages that have failed
    processing and been moved to the Dead Letter Queue.

    Requires the following attributes on the parent class:
    - table_name: str - Base table name
    - _get_connection() -> sqlite3.Connection - Get database connection
    - _pending: dict[int, dict] - Pending messages tracker
    """

    table_name: str
    _pending: dict[int, dict[str, Any]]

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection (to be implemented by parent class)."""
        raise NotImplementedError

    def move_to_dlq(
        self,
        message_id: int | str,
        error: str | None = None,
    ) -> None:
        """Move a message to the Dead Letter Queue.

        Called when a message exceeds max_attempts or encounters a
        permanent (non-retryable) error.

        Args:
            message_id: The message ID (integer or string)
            error: Error message/details explaining why it was moved
        """
        msg_id = int(message_id)
        conn = self._get_connection()

        cursor = conn.execute(
            f"""
            DELETE FROM {self.table_name}
            WHERE id = :id
            RETURNING id, message_id, message_type, payload, attempts, created_at
            """,
            {"id": msg_id},
        )
        row = cursor.fetchone()

        if not row:
            logger.warning("Message %s not found for DLQ move", msg_id)
            return

        conn.execute(
            f"""
            INSERT INTO {self.table_name}_dlq (
                original_id, message_id, message_type, payload,
                attempts, error, last_error_at, created_at
            ) VALUES (
                :original_id, :message_id, :message_type, :payload,
                :attempts, :error, datetime('now', 'utc'), :created_at
            )
            """,
            {
                "original_id": row["id"],
                "message_id": row["message_id"],
                "message_type": row["message_type"],
                "payload": row["payload"],
                "attempts": row["attempts"],
                "error": error or "Max attempts exceeded",
                "created_at": row["created_at"],
            },
        )
        conn.commit()

        self._pending.pop(msg_id, None)
        logger.warning(
            "Moved message to DLQ (id=%s, type=%s, attempts=%s, error=%s)",
            msg_id,
            row["message_type"],
            row["attempts"],
            error,
        )

    def list_dlq(
        self,
        limit: int = 100,
        message_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """List messages in the Dead Letter Queue.

        Args:
            limit: Maximum number of messages to return
            message_type: Optional filter by message type

        Returns:
            List of DLQ message dictionaries with keys:
                - id: DLQ entry ID
                - original_id: Original queue message ID
                - message_id: UUID of the message
                - message_type: Type of the message
                - payload: JSON payload
                - attempts: Number of attempts before failure
                - error: Error message
                - created_at: When the message was originally created
                - moved_at: When it was moved to DLQ
        """
        conn = self._get_connection()

        query = f"SELECT * FROM {self.table_name}_dlq"
        params: dict[str, Any] = {"limit": limit}

        if message_type:
            query += " WHERE message_type = :message_type"
            params["message_type"] = message_type

        query += " ORDER BY moved_at DESC LIMIT :limit"

        result = conn.execute(query, params)
        return [dict(row) for row in result.fetchall()]

    def replay_dlq(self, dlq_id: int) -> bool:
        """Replay a message from the DLQ back to the main queue.

        The message is moved back to the main queue with attempts reset
        to 0, giving it another chance to process.

        Args:
            dlq_id: The DLQ entry ID

        Returns:
            True if replayed successfully, False if not found
        """
        conn = self._get_connection()

        cursor = conn.execute(
            f"DELETE FROM {self.table_name}_dlq WHERE id = :id RETURNING *",
            {"id": dlq_id},
        )
        row = cursor.fetchone()

        if not row:
            logger.warning("DLQ entry %s not found for replay", dlq_id)
            return False

        conn.execute(
            f"""
            INSERT INTO {self.table_name} (
                message_id, message_type, payload, deliver_at, attempts
            ) VALUES (
                :message_id, :message_type, :payload, datetime('now', 'utc'), 0
            )
            """,
            {
                "message_id": row["message_id"] + "-replay",
                "message_type": row["message_type"],
                "payload": row["payload"],
            },
        )
        conn.commit()

        logger.info("Replayed DLQ entry %s (type=%s)", dlq_id, row["message_type"])
        return True

    def dlq_size(self) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        conn = self._get_connection()
        result = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}_dlq")
        row = result.fetchone()
        return row[0] if row else 0

    def clear_dlq(self) -> int:
        """Clear all messages from the Dead Letter Queue.

        Returns:
            Number of messages cleared
        """
        conn = self._get_connection()
        count = self.dlq_size()
        conn.execute(f"DELETE FROM {self.table_name}_dlq")
        conn.commit()
        logger.info("Cleared %d messages from DLQ", count)
        return count

    def check_and_move_expired(self) -> int:
        """Check for messages exceeding max_attempts and move to DLQ.

        This is called by the processor to clean up failed messages.

        Returns:
            Number of messages moved to DLQ
        """
        conn = self._get_connection()

        # Find messages that have exceeded max_attempts
        result = conn.execute(
            f"""
            SELECT id, message_type, attempts
            FROM {self.table_name}
            WHERE attempts >= max_attempts
            """,
        )
        rows = result.fetchall()

        moved_count = 0
        for row in rows:
            self.move_to_dlq(
                row["id"],
                f"Exceeded max_attempts ({row['attempts']})",
            )
            moved_count += 1

        if moved_count > 0:
            logger.info("Moved %d expired messages to DLQ", moved_count)

        return moved_count
