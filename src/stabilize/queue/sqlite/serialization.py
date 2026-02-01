"""Message serialization for SQLite queue."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from stabilize.queue.messages import Message, create_message_from_dict

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
    from stabilize.models.stage import SyntheticStageOwner
    from stabilize.models.status import WorkflowStatus

    try:
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
