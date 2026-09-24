"""Message serialization for SQLite queue."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from stabilize.queue.decode import MessageDecodeError, decode_message
from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)


def serialize_message(message: Message) -> str:
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


def deserialize_message(type_name: str, payload: Any) -> Message | None:
    """Deserialize a message from JSON string or dict.

    Returns None if deserialization fails (corrupted message).
    """
    try:
        return decode_message(type_name, payload)
    except MessageDecodeError as exc:
        logger.error("Failed to decode queue message: %s", exc)
        return None
