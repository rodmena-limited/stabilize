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
