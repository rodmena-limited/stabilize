"""
Queue interface and common types.

This module provides the abstract Queue interface and common types
used by all queue implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from stabilize.queue.messages import Message


class QueueFullError(Exception):
    """Raised when the queue is full and overflow policy is 'raise'."""

    pass


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
        connection: Any | None = None,
    ) -> None:
        """
        Push a message onto the queue.

        Args:
            message: The message to push
            delay: Optional delay before message is delivered
            connection: Optional database connection for atomic transactions
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

    def has_pending_message_for_task(self, task_id: str) -> bool:
        """Check if there's already a pending message for a specific task.

        Used by recovery to prevent creating duplicate messages.
        Default implementation returns False (no deduplication).
        Queue implementations should override this for proper recovery behavior.

        Args:
            task_id: The task ID to check for

        Returns:
            True if a pending message exists for this task
        """
        return False


@dataclass(order=True)
class QueuedMessage:
    """A message with its delivery time for priority queue ordering."""

    deliver_at: float
    message: Message = field(compare=False)
    message_id: str = field(compare=False, default="")
