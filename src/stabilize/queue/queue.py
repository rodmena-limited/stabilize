"""
Queue interface and implementations.

This module provides the abstract Queue interface and concrete implementations
for different backends (in-memory, PostgreSQL).
"""

from __future__ import annotations

import heapq
import json
import logging
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from stabilize.queue.messages import (
    Message,
    create_message_from_dict,
    get_message_type_name,
)

logger = logging.getLogger(__name__)


class Queue(ABC):
    """
    Abstract queue interface for message handling.

    The queue is the core of the execution engine, managing all messages
    that drive stage and task execution.
    """

    @abstractmethod
    def push(
        self,
        message: Message,
        delay: timedelta | None = None,
    ) -> None:
        """
        Push a message onto the queue.

        Args:
            message: The message to push
            delay: Optional delay before message is delivered
        """
        pass

    @abstractmethod
    def poll(self, callback: Callable[[Message], None]) -> None:
        """
        Poll for a message and process it with the callback.

        If a message is available, calls callback(message).
        After callback returns, the message is automatically acknowledged.

        Args:
            callback: Function to call with the message
        """
        pass

    @abstractmethod
    def poll_one(self) -> Message | None:
        """
        Poll for a single message without callback.

        Returns the message if available, None otherwise.
        Message must be manually acknowledged.

        Returns:
            The message or None
        """
        pass

    @abstractmethod
    def ack(self, message: Message) -> None:
        """
        Acknowledge a message, removing it from the queue.

        Args:
            message: The message to acknowledge
        """
        pass

    @abstractmethod
    def ensure(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """
        Ensure a message is in the queue with the given delay.

        If the message is already in the queue, updates its delay.
        If not, adds it with the given delay.

        Args:
            message: The message to ensure
            delay: Delay before message is delivered
        """
        pass

    @abstractmethod
    def reschedule(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """
        Reschedule a message with a new delay.

        Args:
            message: The message to reschedule
            delay: New delay before message is delivered
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """Get the number of messages in the queue."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all messages from the queue."""
        pass


@dataclass(order=True)
class QueuedMessage:
    """A message with its delivery time for priority queue ordering."""

    deliver_at: float
    message: Message = field(compare=False)
    message_id: str = field(compare=False, default="")


class InMemoryQueue(Queue):
    """
    In-memory queue implementation using a priority queue.

    Useful for testing and single-process execution.
    Messages are ordered by delivery time.
    """

    def __init__(self) -> None:
        self._queue: list[QueuedMessage] = []
        self._lock = threading.Lock()
        self._message_id_counter = 0
        self._pending: dict[str, QueuedMessage] = {}  # Messages being processed
        self._message_index: dict[str, QueuedMessage] = {}  # For ensure/reschedule

    def _generate_message_id(self) -> str:
        """Generate a unique message ID."""
        self._message_id_counter += 1
        return f"msg-{self._message_id_counter}"

    def push(
        self,
        message: Message,
        delay: timedelta | None = None,
    ) -> None:
        """Push a message onto the queue."""
        with self._lock:
            deliver_at = time.time()
            if delay:
                deliver_at += delay.total_seconds()

            message_id = self._generate_message_id()
            message.message_id = message_id

            queued = QueuedMessage(
                deliver_at=deliver_at,
                message=message,
                message_id=message_id,
            )

            heapq.heappush(self._queue, queued)
            self._message_index[message_id] = queued

            logger.debug(
                f"Pushed {get_message_type_name(message)} "
                f"(id={message_id}, deliver_at={datetime.fromtimestamp(deliver_at)})"
            )

    def poll(self, callback: Callable[[Message], None]) -> None:
        """Poll for a message and process it with the callback."""
        message = self.poll_one()
        if message:
            try:
                callback(message)
            finally:
                self.ack(message)

    def poll_one(self) -> Message | None:
        """Poll for a single message without callback."""
        with self._lock:
            now = time.time()

            # Skip messages that aren't ready yet
            while self._queue and self._queue[0].deliver_at <= now:
                queued = heapq.heappop(self._queue)
                message_id = queued.message_id

                # Remove from index
                self._message_index.pop(message_id, None)

                # Add to pending
                self._pending[message_id] = queued

                logger.debug(f"Polled {get_message_type_name(queued.message)} (id={message_id})")

                return queued.message

            return None

    def ack(self, message: Message) -> None:
        """Acknowledge a message, removing it from pending."""
        with self._lock:
            message_id = message.message_id
            if message_id and message_id in self._pending:
                del self._pending[message_id]
                logger.debug(f"Acked {get_message_type_name(message)} (id={message_id})")

    def ensure(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """Ensure a message is in the queue with the given delay."""
        # For in-memory queue, just push the message
        # In production, would check if similar message exists
        self.push(message, delay)

    def reschedule(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """Reschedule a message with a new delay."""
        with self._lock:
            message_id = message.message_id
            if message_id and message_id in self._pending:
                # Remove from pending
                del self._pending[message_id]

        # Push with new delay
        self.push(message, delay)

    def size(self) -> int:
        """Get the number of messages in the queue."""
        with self._lock:
            return len(self._queue) + len(self._pending)

    def ready_count(self) -> int:
        """Get the number of messages ready to be delivered."""
        with self._lock:
            now = time.time()
            return sum(1 for q in self._queue if q.deliver_at <= now)

    def clear(self) -> None:
        """Clear all messages from the queue."""
        with self._lock:
            self._queue.clear()
            self._pending.clear()
            self._message_index.clear()


class PostgresQueue(Queue):
    """
    PostgreSQL-backed queue implementation.

    Uses FOR UPDATE SKIP LOCKED for concurrent message processing across
    multiple workers. Messages are stored in a table with delivery time.

    Connection pools are managed by singleton ConnectionManager for
    efficient resource sharing across all queue instances.
    """

    def __init__(
        self,
        connection_string: str,
        table_name: str = "queue_messages",
        lock_duration: timedelta = timedelta(seconds=60),
        max_attempts: int = 10,
    ) -> None:
        """
        Initialize the PostgreSQL queue.

        Args:
            connection_string: PostgreSQL connection string
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

    def _get_pool(self) -> Any:
        """Get the shared connection pool from ConnectionManager."""
        return self._manager.get_postgres_pool(self.connection_string)

    def close(self) -> None:
        """Close the connection pool via connection manager."""
        self._manager.close_postgres_pool(self.connection_string)

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
                data[key] = value.name  # Use name, not value (which may be a tuple)
            else:
                data[key] = value
        return json.dumps(data)

    def _deserialize_message(self, type_name: str, payload: Any) -> Message:
        """Deserialize a message from JSON or dict."""
        from stabilize.models.stage import SyntheticStageOwner
        from stabilize.models.status import WorkflowStatus

        # psycopg3 returns JSONB as dict directly
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
        pool = self._get_pool()
        deliver_at = datetime.now()
        if delay:
            deliver_at += delay

        message_type = get_message_type_name(message)
        message_id = str(uuid.uuid4())
        payload = self._serialize_message(message)

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.table_name}
                    (message_id, message_type, payload, deliver_at, attempts)
                    VALUES (%(message_id)s, %(type)s, %(payload)s::jsonb, %(deliver_at)s, 0)
                    """,
                    {
                        "message_id": message_id,
                        "type": message_type,
                        "payload": payload,
                        "deliver_at": deliver_at,
                    },
                )
            conn.commit()

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
        """Poll for a single message without callback."""
        pool = self._get_pool()
        locked_until = datetime.now() + self.lock_duration

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Use SKIP LOCKED to allow concurrent workers
                cur.execute(
                    f"""
                    UPDATE {self.table_name}
                    SET locked_until = %(locked_until)s,
                        attempts = attempts + 1
                    WHERE id = (
                        SELECT id FROM {self.table_name}
                        WHERE deliver_at <= NOW()
                        AND (locked_until IS NULL OR locked_until < NOW())
                        AND attempts < %(max_attempts)s
                        ORDER BY deliver_at
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, message_type, payload, attempts
                    """,
                    {
                        "locked_until": locked_until,
                        "max_attempts": self.max_attempts,
                    },
                )
                row = cur.fetchone()
            conn.commit()

            if row:
                msg_id = row["id"]
                msg_type = row["message_type"]
                payload = row["payload"]
                attempts = row["attempts"]

                message = self._deserialize_message(msg_type, payload)
                message.message_id = str(msg_id)
                message.attempts = attempts
                self._pending[msg_id] = {
                    "message": message,
                    "type": msg_type,
                }
                return message

            return None

    def ack(self, message: Message) -> None:
        """Acknowledge a message, removing it from the queue."""
        if not message.message_id:
            return

        msg_id = int(message.message_id)
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.table_name} WHERE id = %(id)s",
                    {"id": msg_id},
                )
            conn.commit()

        self._pending.pop(msg_id, None)

    def ensure(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """Ensure a message is in the queue with the given delay."""
        # For simplicity, just push the message
        # In production, would check for duplicates
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
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.table_name}
                    SET deliver_at = %(deliver_at)s,
                        locked_until = NULL
                    WHERE id = %(id)s
                    """,
                    {"id": msg_id, "deliver_at": deliver_at},
                )
            conn.commit()

        self._pending.pop(msg_id, None)

    def size(self) -> int:
        """Get the number of messages in the queue."""
        pool = self._get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as cnt FROM {self.table_name}")
                row = cur.fetchone()
                return row["cnt"] if row else 0

    def clear(self) -> None:
        """Clear all messages from the queue."""
        pool = self._get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self.table_name}")
            conn.commit()

        self._pending.clear()

    # ========== Dead Letter Queue Methods ==========

    def move_to_dlq(
        self,
        message_id: int | str,
        error: str | None = None,
    ) -> None:
        """Move a message to the Dead Letter Queue.

        Args:
            message_id: The message ID (integer or string)
            error: Error message/details explaining why it was moved
        """
        msg_id = int(message_id)
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get the message from main queue
                cur.execute(
                    f"""
                    SELECT id, message_id, message_type, payload, attempts, created_at
                    FROM {self.table_name}
                    WHERE id = %(id)s
                    """,
                    {"id": msg_id},
                )
                row = cur.fetchone()

                if not row:
                    logger.warning(f"Message {msg_id} not found for DLQ move")
                    return

                # Insert into DLQ
                cur.execute(
                    f"""
                    INSERT INTO {self.table_name}_dlq (
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
                        "payload": json.dumps(row["payload"]) if isinstance(row["payload"], dict) else row["payload"],
                        "attempts": row["attempts"],
                        "error": error or "Max attempts exceeded",
                        "created_at": row["created_at"],
                    },
                )

                # Delete from main queue
                cur.execute(
                    f"DELETE FROM {self.table_name} WHERE id = %(id)s",
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
            List of DLQ message dictionaries
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                if message_type:
                    cur.execute(
                        f"""
                        SELECT * FROM {self.table_name}_dlq
                        WHERE message_type = %(message_type)s
                        ORDER BY moved_at DESC
                        LIMIT %(limit)s
                        """,
                        {"message_type": message_type, "limit": limit},
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT * FROM {self.table_name}_dlq
                        ORDER BY moved_at DESC
                        LIMIT %(limit)s
                        """,
                        {"limit": limit},
                    )
                rows = cur.fetchall()
                return [dict(row) for row in rows]

    def replay_dlq(self, dlq_id: int) -> bool:
        """Replay a message from the DLQ back to the main queue.

        Args:
            dlq_id: The DLQ entry ID

        Returns:
            True if replayed successfully, False if not found
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Get the DLQ entry
                cur.execute(
                    f"SELECT * FROM {self.table_name}_dlq WHERE id = %(id)s",
                    {"id": dlq_id},
                )
                row = cur.fetchone()

                if not row:
                    logger.warning(f"DLQ entry {dlq_id} not found for replay")
                    return False

                # Insert back into main queue with reset attempts
                cur.execute(
                    f"""
                    INSERT INTO {self.table_name} (
                        message_id, message_type, payload, deliver_at, attempts
                    ) VALUES (
                        %(message_id)s, %(message_type)s, %(payload)s::jsonb, NOW(), 0
                    )
                    """,
                    {
                        "message_id": row["message_id"] + "-replay",
                        "message_type": row["message_type"],
                        "payload": json.dumps(row["payload"]) if isinstance(row["payload"], dict) else row["payload"],
                    },
                )

                # Delete from DLQ
                cur.execute(
                    f"DELETE FROM {self.table_name}_dlq WHERE id = %(id)s",
                    {"id": dlq_id},
                )
            conn.commit()

        logger.info(f"Replayed DLQ entry {dlq_id} (type={row['message_type']})")
        return True

    def dlq_size(self) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        pool = self._get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as cnt FROM {self.table_name}_dlq")
                row = cur.fetchone()
                return row["cnt"] if row else 0

    def clear_dlq(self) -> int:
        """Clear all messages from the Dead Letter Queue.

        Returns:
            Number of messages cleared
        """
        pool = self._get_pool()
        count = self.dlq_size()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self.table_name}_dlq")
            conn.commit()
        logger.info(f"Cleared {count} messages from DLQ")
        return count

    def check_and_move_expired(self) -> int:
        """Check for messages exceeding max_attempts and move to DLQ.

        Returns:
            Number of messages moved to DLQ
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Find messages that have exceeded max_attempts
                cur.execute(
                    f"""
                    SELECT id, message_type, attempts
                    FROM {self.table_name}
                    WHERE attempts >= %(max_attempts)s
                    """,
                    {"max_attempts": self.max_attempts},
                )
                rows = cur.fetchall()

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
