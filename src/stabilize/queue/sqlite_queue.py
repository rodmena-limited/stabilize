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

import json
import logging
import sqlite3
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from stabilize.queue.messages import (
    Message,
    create_message_from_dict,
    get_message_type_name,
)
from stabilize.queue.queue import Queue

logger = logging.getLogger(__name__)


class SqliteQueue(Queue):
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

        # Main queue table
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                message_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                deliver_at TEXT NOT NULL DEFAULT (datetime('now')),
                attempts INTEGER DEFAULT 0,
                max_attempts INTEGER DEFAULT 10,
                locked_until TEXT,
                version INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_deliver
            ON {self.table_name}(deliver_at)
        """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_locked
            ON {self.table_name}(locked_until)
        """
        )

        # Dead Letter Queue table
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}_dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_id INTEGER,
                message_id TEXT NOT NULL,
                message_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                attempts INTEGER,
                error TEXT,
                last_error_at TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                moved_at TEXT DEFAULT (datetime('now'))
            )
        """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{self.table_name}_dlq_type
            ON {self.table_name}_dlq(message_type)
        """
        )
        conn.commit()

    def _serialize_message(self, message: Message) -> str:
        """Serialize a message to JSON."""
        from enum import Enum

        data = {}
        for key, value in message.__dict__.items():
            if key.startswith("_"):
                continue
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif isinstance(value, Enum):
                data[key] = value.name
            else:
                data[key] = value
        return json.dumps(data)

    def _deserialize_message(self, type_name: str, payload: Any) -> Message:
        """Deserialize a message from JSON string or dict."""
        from stabilize.models.stage import SyntheticStageOwner
        from stabilize.models.status import WorkflowStatus

        if isinstance(payload, dict):
            data = payload
        else:
            data = json.loads(payload)

        # Convert enum values
        if "status" in data and isinstance(data["status"], str):
            data["status"] = WorkflowStatus[data["status"]]
        if "original_status" in data and data["original_status"]:
            data["original_status"] = WorkflowStatus[data["original_status"]]
        if "phase" in data and isinstance(data["phase"], str):
            data["phase"] = SyntheticStageOwner[data["phase"]]

        # Remove metadata fields
        data.pop("message_id", None)
        data.pop("created_at", None)
        data.pop("attempts", None)
        data.pop("max_attempts", None)

        return create_message_from_dict(type_name, data)

    def push(
        self,
        message: Message,
        delay: timedelta | None = None,
    ) -> None:
        """Push a message onto the queue."""
        conn = self._get_connection()
        deliver_at = datetime.now()
        if delay:
            deliver_at += delay

        message_type = get_message_type_name(message)
        message_id = str(uuid.uuid4())
        payload = self._serialize_message(message)

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

        logger.debug(f"Pushed {message_type} (id={message_id}, deliver_at={deliver_at})")

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
        locked_until = datetime.now() + self.lock_duration

        # Step 1: Find a candidate message
        result = conn.execute(
            f"""
            SELECT id, message_type, payload, attempts, version
            FROM {self.table_name}
            WHERE datetime(deliver_at) <= datetime('now')
            AND (locked_until IS NULL OR datetime(locked_until) < datetime('now'))
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
            logger.debug(f"Lost race for message {msg_id}, will retry")
            return None

        # Step 4: Successfully claimed - deserialize and return
        message = self._deserialize_message(msg_type, payload)
        message.message_id = str(msg_id)
        message.attempts = attempts + 1

        self._pending[msg_id] = {
            "message": message,
            "type": msg_type,
        }

        logger.debug(f"Polled {msg_type} (id={msg_id}, attempts={attempts + 1})")
        return message

    def ack(self, message: Message) -> None:
        """Acknowledge a message, removing it from the queue."""
        if not message.message_id:
            return

        msg_id = int(message.message_id)
        conn = self._get_connection()

        conn.execute(
            f"DELETE FROM {self.table_name} WHERE id = :id",
            {"id": msg_id},
        )
        conn.commit()

        self._pending.pop(msg_id, None)
        logger.debug(f"Acked message (id={msg_id})")

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
        deliver_at = datetime.now() + delay
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
        logger.debug(f"Rescheduled message (id={msg_id}, deliver_at={deliver_at})")

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

    # ========== Dead Letter Queue Methods ==========

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

        # Get the message from main queue
        result = conn.execute(
            f"""
            SELECT id, message_id, message_type, payload, attempts, created_at
            FROM {self.table_name}
            WHERE id = :id
            """,
            {"id": msg_id},
        )
        row = result.fetchone()

        if not row:
            logger.warning(f"Message {msg_id} not found for DLQ move")
            return

        # Insert into DLQ
        conn.execute(
            f"""
            INSERT INTO {self.table_name}_dlq (
                original_id, message_id, message_type, payload,
                attempts, error, last_error_at, created_at
            ) VALUES (
                :original_id, :message_id, :message_type, :payload,
                :attempts, :error, datetime('now'), :created_at
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

        # Delete from main queue
        conn.execute(
            f"DELETE FROM {self.table_name} WHERE id = :id",
            {"id": msg_id},
        )
        conn.commit()

        self._pending.pop(msg_id, None)
        logger.warning(
            f"Moved message to DLQ (id={msg_id}, type={row['message_type']}, attempts={row['attempts']}, error={error})"
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

        # Get the DLQ entry
        result = conn.execute(
            f"SELECT * FROM {self.table_name}_dlq WHERE id = :id",
            {"id": dlq_id},
        )
        row = result.fetchone()

        if not row:
            logger.warning(f"DLQ entry {dlq_id} not found for replay")
            return False

        # Insert back into main queue with reset attempts
        conn.execute(
            f"""
            INSERT INTO {self.table_name} (
                message_id, message_type, payload, deliver_at, attempts
            ) VALUES (
                :message_id, :message_type, :payload, datetime('now'), 0
            )
            """,
            {
                "message_id": row["message_id"] + "-replay",
                "message_type": row["message_type"],
                "payload": row["payload"],
            },
        )

        # Delete from DLQ
        conn.execute(
            f"DELETE FROM {self.table_name}_dlq WHERE id = :id",
            {"id": dlq_id},
        )
        conn.commit()

        logger.info(f"Replayed DLQ entry {dlq_id} (type={row['message_type']})")
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
        logger.info(f"Cleared {count} messages from DLQ")
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
            logger.info(f"Moved {moved_count} expired messages to DLQ")

        return moved_count
