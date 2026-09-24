"""Turn a stored queue row back into a Message."""

from __future__ import annotations

import json
from typing import Any

from stabilize.queue.messages import Message, create_message_from_dict

_METADATA_FIELDS = ("message_id", "created_at", "attempts", "max_attempts")


class MessageDecodeError(ValueError):
    """A stored queue row could not be turned back into a message."""


def _reason(exc: Exception) -> str:
    if isinstance(exc, KeyError):
        return f"unknown enum name {exc.args[0]!r}" if exc.args else "unknown enum name"
    return f"{type(exc).__name__}: {exc}"


def decode_message(type_name: str, payload: Any) -> Message:
    """Decode one queue row.

    Raises:
        MessageDecodeError: for any failure, with a reason naming the cause.
    """
    from stabilize.models.stage import SyntheticStageOwner
    from stabilize.models.status import WorkflowStatus

    try:
        data = dict(payload) if isinstance(payload, dict) else json.loads(payload)
        if not isinstance(data, dict):
            raise TypeError(f"payload is {type(data).__name__}, not an object")
        if isinstance(data.get("status"), str):
            data["status"] = WorkflowStatus[data["status"]]
        if data.get("original_status"):
            data["original_status"] = WorkflowStatus[data["original_status"]]
        if isinstance(data.get("phase"), str):
            data["phase"] = SyntheticStageOwner[data["phase"]]
        for key in _METADATA_FIELDS:
            data.pop(key, None)
        return create_message_from_dict(type_name, data)
    except Exception as exc:
        raise MessageDecodeError(f"{type_name}: {_reason(exc)}") from None
