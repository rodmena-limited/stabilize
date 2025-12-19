from __future__ import annotations
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeVar
from stabilize.queue.messages import Message, get_message_type_name
from stabilize.queue.queue import Queue
logger = logging.getLogger(__name__)
M = TypeVar("M", bound=Message)

class MessageHandler(Generic[M]):
    """
    Base class for message handlers.

    Each handler processes a specific type of message.
    """

    def message_type(self) -> type[M]:
        """Return the type of message this handler processes."""
        raise NotImplementedError

    def handle(self, message: M) -> None:
        """
        Handle a message.

        Args:
            message: The message to handle
        """
        raise NotImplementedError

@dataclass
class QueueProcessorConfig:
    """Configuration for the queue processor."""
    poll_frequency_ms: int = 50
    max_workers: int = 10
    retry_delay: timedelta = timedelta(seconds=15)
    stop_on_error: bool = False
