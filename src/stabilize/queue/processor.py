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
