"""Dead Letter Queue mixin for SQLite queue."""

from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from stabilize.persistence.columns import select_list
from stabilize.queue.message_ids import row_id

_DLQ = select_list("queue_messages_dlq", "sqlite")

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class SqliteDLQMixin:
    tenant: str
    """Mixin providing Dead Letter Queue functionality for SQLite queue.

    This mixin provides methods for managing messages that have failed
    processing and been moved to the Dead Letter Queue.

    Requires the following attributes on the parent class:
    - table_name: str - Base table name
    - _get_connection() -> sqlite3.Connection - Get database connection
    - _pending: dict[int, dict] - Pending messages tracker
    """

    table_name: str
    max_attempts: int
    store_retry_budget: timedelta
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
        msg_id = row_id(message_id)
        conn = self._get_connection()

        cursor = conn.execute(
            f"""
            DELETE FROM {self.table_name}
            WHERE id = :id AND stabilize_tenant = :tenant
            RETURNING id, message_id, message_type, payload, attempts, created_at, stabilize_tenant,
                      store_retries, store_retry_since
            """,
            {"id": msg_id, "tenant": self.tenant},
        )
        row = cursor.fetchone()

        if not row:
            logger.warning("Message %s not found for DLQ move", msg_id)
            return

        conn.execute(
            f"""
            INSERT INTO {self.table_name}_dlq (
                original_id, message_id, message_type, payload,
                attempts, error, last_error_at, created_at, stabilize_tenant,
                store_retries, store_retry_since
            ) VALUES (
                :original_id, :message_id, :message_type, :payload,
                :attempts, :error, datetime('now', 'utc'), :created_at, :tenant,
                :store_retries, :store_retry_since
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
                "tenant": row["stabilize_tenant"],
                "store_retries": row["store_retries"],
                "store_retry_since": row["store_retry_since"],
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

        query = f"SELECT {_DLQ} FROM {self.table_name}_dlq WHERE stabilize_tenant = :tenant"
        params: dict[str, Any] = {"limit": limit, "tenant": self.tenant}

        if message_type:
            query += " AND message_type = :message_type"
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
            f"DELETE FROM {self.table_name}_dlq WHERE id = :id AND stabilize_tenant = :tenant RETURNING {_DLQ}",
            {"id": dlq_id, "tenant": self.tenant},
        )
        row = cursor.fetchone()

        if not row:
            logger.warning("DLQ entry %s not found for replay", dlq_id)
            return False

        conn.execute(
            f"""
            INSERT INTO {self.table_name} (
                message_id, message_type, payload, deliver_at, attempts, stabilize_tenant
            ) VALUES (
                :message_id, :message_type, :payload, :now, 0, :tenant
            )
            """,
            {
                "message_id": str(uuid.uuid4()),
                "message_type": row["message_type"],
                "payload": row["payload"],
                "now": datetime.now(UTC).isoformat(),
                "tenant": row["stabilize_tenant"],
            },
        )
        conn.commit()

        logger.info("Replayed DLQ entry %s (type=%s)", dlq_id, row["message_type"])
        return True

    def dlq_size(self) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        conn = self._get_connection()
        result = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}_dlq WHERE stabilize_tenant = ?", (self.tenant,))
        row = result.fetchone()
        return row[0] if row else 0

    def clear_dlq(self) -> int:
        """Clear all messages from the Dead Letter Queue.

        Returns:
            Number of messages cleared
        """
        conn = self._get_connection()
        count = self.dlq_size()
        conn.execute(f"DELETE FROM {self.table_name}_dlq WHERE stabilize_tenant = ?", (self.tenant,))
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
            SELECT id, message_type, attempts, store_retries, store_retry_since IS NOT NULL AS over_budget
            FROM {self.table_name}
            WHERE stabilize_tenant = :tenant
            AND (locked_until IS NULL OR julianday(locked_until) < julianday(:now))
            AND attempts >= :max_attempts
            AND (store_retry_since IS NULL OR julianday(store_retry_since) <= julianday(:now) - :budget_days)
            """,
            {
                "max_attempts": self.max_attempts,
                "tenant": self.tenant,
                "now": datetime.now(UTC).isoformat(),
                "budget_days": self.store_retry_budget.total_seconds() / 86400.0,
            },
        )
        rows = result.fetchall()

        moved_count = 0
        for row in rows:
            from stabilize.queue.dlq import dead_letter_cause

            self.move_to_dlq(row["id"], dead_letter_cause(row))
            moved_count += 1

        if moved_count > 0:
            logger.info("Moved %d expired messages to DLQ", moved_count)

        return moved_count
