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

    def poll(self, callback: Callable[[Message], None]) -> None:
        """
        Poll for a message and process it with the callback.

        If a message is available, calls callback(message).
        After callback returns, the message is automatically acknowledged.

        Args:
            callback: Function to call with the message
        """
        pass

    def poll_one(self) -> Message | None:
        """
        Poll for a single message without callback.

        Returns the message if available, None otherwise.
        Message must be manually acknowledged.

        Returns:
            The message or None
        """
        pass

    def ack(self, message: Message) -> None:
        """
        Acknowledge a message, removing it from the queue.

        Args:
            message: The message to acknowledge
        """
        pass

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

    def size(self) -> int:
        """Get the number of messages in the queue."""
        pass

    def clear(self) -> None:
        """Clear all messages from the queue."""
        pass

@dataclass(order=True)
class QueuedMessage:
    """A message with its delivery time for priority queue ordering."""
    deliver_at: float
    message: Message = field(compare=False)
    message_id: str = field(compare=False, default='')

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
        lock_duration: timedelta = timedelta(minutes=5),
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
