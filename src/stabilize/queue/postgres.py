"""
PostgreSQL-backed queue implementation.

Uses FOR UPDATE SKIP LOCKED for concurrent message processing across
multiple workers. Messages are stored in a table with delivery time.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from stabilize.queue.interface import Queue
from stabilize.queue.messages import (
    Message,
    create_message_from_dict,
    get_message_type_name,
)

logger = logging.getLogger(__name__)


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
        # Size caching
        self._size_cache: int | None = None
        self._last_size_check: float = 0
        self._size_cache_ttl: float = 5.0  # seconds

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

    def _deserialize_message(self, type_name: str, payload: Any) -> Message | None:
        """Deserialize a message from JSON or dict.

        Returns None if deserialization fails (corrupted message).
        """
        from stabilize.models.stage import SyntheticStageOwner
        from stabilize.models.status import WorkflowStatus

        try:
            # psycopg3 returns JSONB as dict directly
            if isinstance(payload, dict):
                data = payload
            else:
                data = json.loads(payload)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(
                "Failed to decode message payload: %s. Payload: %s",
                e,
                payload[:200] if isinstance(payload, str) else payload,
            )
            return None

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
        connection: Any | None = None,
    ) -> None:
        """Push a message onto the queue."""
        pool = self._get_pool()

        message_type = get_message_type_name(message)
        message_id = str(uuid.uuid4())
        payload = self._serialize_message(message)

        # Use PostgreSQL's NOW() for deliver_at to avoid clock drift between Python and PostgreSQL.
        # When comparing timestamps, we need to ensure consistency with the poll query which uses NOW().
        if delay:
            delay_seconds = delay.total_seconds()
            deliver_at_sql = f"NOW() + INTERVAL '{delay_seconds} seconds'"
        else:
            deliver_at_sql = "NOW()"

        # Use provided connection or get one from pool
        # If provided, caller handles commit/rollback
        if connection:
            with connection.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.table_name}
                    (message_id, message_type, payload, deliver_at, attempts)
                    VALUES (%(message_id)s, %(type)s, %(payload)s::jsonb, {deliver_at_sql}, 0)
                    """,
                    {
                        "message_id": message_id,
                        "type": message_type,
                        "payload": payload,
                    },
                )
        else:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {self.table_name}
                        (message_id, message_type, payload, deliver_at, attempts)
                        VALUES (%(message_id)s, %(type)s, %(payload)s::jsonb, {deliver_at_sql}, 0)
                        """,
                        {
                            "message_id": message_id,
                            "type": message_type,
                            "payload": payload,
                        },
                    )
                conn.commit()

        # Invalidate size cache
        self._size_cache = None

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
                if message is None:
                    # Corrupted message - move to DLQ for audit instead of deleting
                    logger.warning(
                        "Moving corrupted message %s (type: %s) to DLQ", msg_id, msg_type
                    )
                    self.move_to_dlq(
                        msg_id,
                        error=f"Deserialization failed for message type: {msg_type}",
                    )
                    return None

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

        try:
            msg_id = int(message.message_id)
        except ValueError:
            logger.error("Invalid message_id format in ack(): %s", message.message_id)
            return

        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.table_name} WHERE id = %(id)s",
                    {"id": msg_id},
                )
            conn.commit()

        self._pending.pop(msg_id, None)
        # Invalidate size cache
        self._size_cache = None

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
        """Get the number of messages in the queue (cached)."""
        now = time.time()
        if self._size_cache is not None and now - self._last_size_check < self._size_cache_ttl:
            return self._size_cache

        pool = self._get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as cnt FROM {self.table_name}")
                row = cur.fetchone()
                count = row["cnt"] if row else 0

        self._size_cache = count
        self._last_size_check = now
        return count

    def clear(self) -> None:
        """Clear all messages from the queue."""
        pool = self._get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self.table_name}")
            conn.commit()

        self._pending.clear()
        # Invalidate size cache
        self._size_cache = None

    def has_pending_message_for_task(self, task_id: str) -> bool:
        """Check if there's already a pending message for a specific task.

        Used by recovery to prevent creating duplicate messages.

        Args:
            task_id: The task ID to check for

        Returns:
            True if a pending message exists for this task
        """
        pool = self._get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT 1 FROM {self.table_name}
                    WHERE payload::text LIKE %s
                    LIMIT 1
                    """,
                    (f'%"task_id": "{task_id}"%',),
                )
                return cur.fetchone() is not None

    # ========== Dead Letter Queue Methods ==========

    def move_to_dlq(
        self,
        message_id: int | str,
        error: str | None = None,
    ) -> None:
        """Move a message to the Dead Letter Queue."""
        from stabilize.queue.dlq import move_to_dlq as _move_to_dlq

        _move_to_dlq(self._get_pool(), self.table_name, self._pending, message_id, error)

    def list_dlq(
        self,
        limit: int = 100,
        message_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """List messages in the Dead Letter Queue."""
        from stabilize.queue.dlq import list_dlq as _list_dlq

        return _list_dlq(self._get_pool(), self.table_name, limit, message_type)

    def replay_dlq(self, dlq_id: int) -> bool:
        """Replay a message from the DLQ back to the main queue."""
        from stabilize.queue.dlq import replay_dlq as _replay_dlq

        return _replay_dlq(self._get_pool(), self.table_name, dlq_id)

    def dlq_size(self) -> int:
        """Get the number of messages in the Dead Letter Queue."""
        from stabilize.queue.dlq import dlq_size as _dlq_size

        return _dlq_size(self._get_pool(), self.table_name)

    def clear_dlq(self) -> int:
        """Clear all messages from the Dead Letter Queue."""
        from stabilize.queue.dlq import clear_dlq as _clear_dlq

        return _clear_dlq(self._get_pool(), self.table_name)

    def check_and_move_expired(self) -> int:
        """Check for messages exceeding max_attempts and move to DLQ."""
        from stabilize.queue.dlq import (
            check_and_move_expired as _check_and_move_expired,
        )

        return _check_and_move_expired(
            self._get_pool(), self.table_name, self._pending, self.max_attempts
        )
