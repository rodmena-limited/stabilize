"""
In-memory queue implementation.

Useful for testing and single-process execution.
"""

from __future__ import annotations

import heapq
import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from stabilize.queue.interface import Queue, QueuedMessage, QueueFullError
from stabilize.queue.messages import Message, get_message_type_name

logger = logging.getLogger(__name__)


class InMemoryQueue(Queue):
    """
    In-memory queue implementation using a priority queue.

    Useful for testing and single-process execution.
    Messages are ordered by delivery time.
    """

    def __init__(self, max_size: int = 10000, overflow_policy: str = "drop") -> None:
        self._queue: list[QueuedMessage] = []
        self._lock = threading.Lock()
        self._message_id_counter = 0
        self._pending: dict[str, QueuedMessage] = {}  # Messages being processed
        self._message_index: dict[str, QueuedMessage] = {}  # For ensure/reschedule
        self.max_size = max_size
        self.overflow_policy = overflow_policy

    def _generate_message_id(self) -> str:
        """Generate a unique message ID."""
        self._message_id_counter += 1
        return f"msg-{self._message_id_counter}"

    def push(
        self,
        message: Message,
        delay: timedelta | None = None,
        connection: Any | None = None,
    ) -> None:
        """Push a message onto the queue."""
        with self._lock:
            # Check size limit (queue + pending)
            current_size = len(self._queue) + len(self._pending)
            if current_size >= self.max_size:
                if self.overflow_policy == "raise":
                    raise QueueFullError(f"Queue full (size={current_size})")

                logger.warning(
                    "Queue full (size=%d), dropping message type %s",
                    current_size,
                    get_message_type_name(message),
                )
                return

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
                "Pushed %s (id=%s, deliver_at=%s)",
                get_message_type_name(message),
                message_id,
                datetime.fromtimestamp(deliver_at),
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

                logger.debug(
                    "Polled %s (id=%s)", get_message_type_name(queued.message), message_id
                )

                return queued.message

            return None

    def ack(self, message: Message) -> None:
        """Acknowledge a message, removing it from pending."""
        with self._lock:
            message_id = message.message_id
            if message_id and message_id in self._pending:
                del self._pending[message_id]
                logger.debug("Acked %s (id=%s)", get_message_type_name(message), message_id)

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
        """Reschedule a message with a new delay.

        Preserves the original message_id to maintain idempotency tracking.
        """
        original_id = message.message_id

        with self._lock:
            if original_id and original_id in self._pending:
                # Remove from pending
                del self._pending[original_id]

            # Calculate new delivery time
            deliver_at = time.time() + delay.total_seconds()

            # Reuse original ID or generate new one
            if original_id:
                message_id = original_id
            else:
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
                "Rescheduled %s (id=%s, deliver_at=%s)",
                get_message_type_name(message),
                message_id,
                datetime.fromtimestamp(deliver_at),
            )

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

    def has_pending_message_for_task(self, task_id: str) -> bool:
        """Check if there's already a pending message for a specific task.

        Used by recovery to prevent creating duplicate messages.

        Args:
            task_id: The task ID to check for

        Returns:
            True if a pending message exists for this task
        """
        with self._lock:
            for qm in self._queue:
                if hasattr(qm.message, "task_id") and qm.message.task_id == task_id:
                    return True
            for qm in self._pending.values():
                if hasattr(qm.message, "task_id") and qm.message.task_id == task_id:
                    return True
        return False
