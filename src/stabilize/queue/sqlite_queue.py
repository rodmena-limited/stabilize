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
        lock_duration: timedelta = timedelta(minutes=5),
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
        """Create the queue table if it doesn't exist."""
        conn = self._get_connection()
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
            (message_id, message_type, payload, deliver_at, attempts)
            VALUES (:message_id, :type, :payload, :deliver_at, 0)
            """,
            {
                "message_id": message_id,
                "type": message_type,
                "payload": payload,
                "deliver_at": deliver_at.isoformat(),
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
