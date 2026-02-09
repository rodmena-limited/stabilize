"""
SQLite-backed queue implementation.

Uses optimistic locking for concurrent message processing since SQLite
does not support FOR UPDATE SKIP LOCKED.
Uses singleton ConnectionManager for efficient connection sharing.

Enterprise Features:
- Dead Letter Queue (DLQ) for failed messages
- Messages exceeding max_attempts are moved to DLQ, not silently dropped
- DLQ can be queried, replayed, or cleared
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from stabilize.queue.interface import Queue
from stabilize.queue.messages import Message, get_message_type_name
from stabilize.queue.sqlite.dlq import SqliteDLQMixin
from stabilize.queue.sqlite.schema import create_queue_tables
from stabilize.queue.sqlite.serialization import deserialize_message, serialize_message

logger = logging.getLogger(__name__)


class SqliteQueue(SqliteDLQMixin, Queue):
    """
    SQLite-backed queue implementation.

    Uses optimistic locking with a version column for concurrent access.
    Multiple workers can safely poll messages, though with lower
    throughput than PostgreSQL's SKIP LOCKED.

    The queue uses a `version` column to implement optimistic locking:
    1. SELECT a candidate message with its version
    2. UPDATE the message WHERE version = expected_version
    3. If 0 rows updated, another worker claimed it - retry

    Features:
    - Multi-worker support via optimistic locking
    - Configurable lock duration and max attempts
    - Automatic retry on lock contention
    - WAL mode for better concurrent read performance
    - Thread-local connections managed by singleton ConnectionManager
    """

    def __init__(
        self,
        connection_string: str,
        table_name: str = "queue_messages",
        lock_duration: timedelta = timedelta(seconds=60),
        max_attempts: int = 10,
    ) -> None:
        """
        Initialize the SQLite queue.

        Args:
            connection_string: SQLite connection string (e.g., sqlite:///./db.sqlite)
            table_name: Name of the queue table
            lock_duration: How long to lock messages during processing
            max_attempts: Maximum retry attempts before dropping message
        """
        from stabilize.persistence.connection import get_connection_manager

        self.connection_string = connection_string
        self.table_name = table_name
        self.lock_duration = lock_duration
        self.max_attempts = max_attempts
        self._manager = get_connection_manager()
        self._pending: dict[int, dict[str, Any]] = {}

    def _get_connection(self) -> sqlite3.Connection:
        """
        Get thread-local connection from ConnectionManager.

        Returns a connection configured with:
        - Row factory for dict-like access
        - WAL journal mode for concurrency
        - 30 second busy timeout
        """
        return self._manager.get_sqlite_connection(self.connection_string)

    def close(self) -> None:
        """Close SQLite connection for current thread."""
        self._manager.close_sqlite_connection(self.connection_string)

    def _create_table(self) -> None:
        """Create the queue table and DLQ table if they don't exist."""
        conn = self._get_connection()
        create_queue_tables(conn, self.table_name)

    def push(
        self,
        message: Message,
        delay: timedelta | None = None,
        connection: Any | None = None,
    ) -> None:
        """Push a message onto the queue."""
        conn = self._get_connection()
        deliver_at = datetime.now(UTC)
        if delay:
            deliver_at += delay

        message_type = get_message_type_name(message)
        message_id = str(uuid.uuid4())
        payload = serialize_message(message)

        conn.execute(
            f"""
            INSERT INTO {self.table_name}
            (message_id, message_type, payload, deliver_at, attempts, max_attempts)
            VALUES (:message_id, :type, :payload, :deliver_at, 0, :max_attempts)
            """,
            {
                "message_id": message_id,
                "type": message_type,
                "payload": payload,
                "deliver_at": deliver_at.isoformat(),
                "max_attempts": self.max_attempts,
            },
        )
        conn.commit()

        logger.debug("Pushed %s (id=%s, deliver_at=%s)", message_type, message_id, deliver_at)

    def poll(self, callback: Callable[[Message], None]) -> None:
        """Poll for a message and process it with the callback."""
        message = self.poll_one()
        if message:
            try:
                callback(message)
                self.ack(message)
            except Exception:
                # Message will be retried after lock expires
                raise

    def poll_one(self) -> Message | None:
        """
        Poll for a single message using optimistic locking.

        This implementation uses a version column to handle concurrent
        access without FOR UPDATE SKIP LOCKED:

        1. SELECT a candidate message with current version
        2. Try to UPDATE with WHERE version = expected
        3. If rowcount == 0, another worker got it - return None
        4. If rowcount == 1, we claimed it successfully

        Returns:
            The claimed message or None if no message available
        """
        conn = self._get_connection()
        locked_until = datetime.now(UTC) + self.lock_duration

        # Step 1: Find a candidate message
        result = conn.execute(
            f"""
            SELECT id, message_type, payload, attempts, version
            FROM {self.table_name}
            WHERE datetime(deliver_at) <= datetime('now', 'utc')
            AND (locked_until IS NULL OR datetime(locked_until) < datetime('now', 'utc'))
            AND attempts < :max_attempts
            ORDER BY deliver_at
            LIMIT 1
            """,
            {"max_attempts": self.max_attempts},
        )
        row = result.fetchone()

        if not row:
            return None

        msg_id = row["id"]
        msg_type = row["message_type"]
        payload = row["payload"]
        attempts = row["attempts"]
        version = row["version"]

        # Step 2: Try to claim with optimistic lock
        cursor = conn.execute(
            f"""
            UPDATE {self.table_name}
            SET locked_until = :locked_until,
                attempts = attempts + 1,
                version = version + 1
            WHERE id = :id AND version = :version
            """,
            {
                "id": msg_id,
                "locked_until": locked_until.isoformat(),
                "version": version,
            },
        )
        conn.commit()

        # Step 3: Check if we won the race
        if cursor.rowcount == 0:
            # Another worker grabbed it
            logger.debug("Lost race for message %s, will retry", msg_id)
            return None

        # Step 4: Successfully claimed - deserialize and return
        message = deserialize_message(msg_type, payload)
        if message is None:
            # Corrupted message - move to DLQ for audit instead of deleting
            logger.warning("Moving corrupted message %s (type: %s) to DLQ", msg_id, msg_type)
            self.move_to_dlq(
                msg_id,
                error=f"Deserialization failed for message type: {msg_type}",
            )
            return None

        message.message_id = str(msg_id)
        message.attempts = attempts + 1

        self._pending[msg_id] = {
            "message": message,
            "type": msg_type,
        }

        logger.debug("Polled %s (id=%s, attempts=%d)", msg_type, msg_id, attempts + 1)
        return message

    def ack(self, message: Message) -> None:
        """Acknowledge a message, removing it from the queue."""
        if not message.message_id:
            return

        try:
            msg_id = int(message.message_id)
        except ValueError:
            logger.error("Invalid message_id format in ack(): %s", message.message_id)
            return

        conn = self._get_connection()

        conn.execute(
            f"DELETE FROM {self.table_name} WHERE id = :id",
            {"id": msg_id},
        )
        conn.commit()

        self._pending.pop(msg_id, None)
        logger.debug("Acked message (id=%s)", msg_id)

    def ensure(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """Ensure a message is in the queue with the given delay."""
        # For simplicity, just push the message
        # A full implementation would check for duplicates
        self.push(message, delay)

    def reschedule(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """Reschedule a message with a new delay."""
        if not message.message_id:
            return

        msg_id = int(message.message_id)
        deliver_at = datetime.now(UTC) + delay
        conn = self._get_connection()

        conn.execute(
            f"""
            UPDATE {self.table_name}
            SET deliver_at = :deliver_at,
                locked_until = NULL
            WHERE id = :id
            """,
            {"id": msg_id, "deliver_at": deliver_at.isoformat()},
        )
        conn.commit()

        self._pending.pop(msg_id, None)
        logger.debug("Rescheduled message (id=%s, deliver_at=%s)", msg_id, deliver_at)

    def size(self) -> int:
        """Get the number of messages in the queue."""
        conn = self._get_connection()
        result = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        row = result.fetchone()
        return row[0] if row else 0

    def clear(self) -> None:
        """Clear all messages from the queue."""
        conn = self._get_connection()
        conn.execute(f"DELETE FROM {self.table_name}")
        conn.commit()

        self._pending.clear()
        logger.debug("Cleared queue")

    def has_pending_message_for_task(self, task_id: str) -> bool:
        """Check if there's already a pending message for a specific task.

        Used by recovery to prevent creating duplicate messages.

        Args:
            task_id: The task ID to check for

        Returns:
            True if a pending message exists for this task
        """
        escaped_id = task_id.replace("%", r"\%").replace("_", r"\_")
        conn = self._get_connection()
        result = conn.execute(
            f"""
            SELECT 1 FROM {self.table_name}
            WHERE payload LIKE :pattern ESCAPE '\\'
            LIMIT 1
            """,
            {"pattern": f'%"task_id": "{escaped_id}"%'},
        )
        return result.fetchone() is not None
