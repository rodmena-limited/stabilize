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
