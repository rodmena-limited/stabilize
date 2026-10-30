from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter

if TYPE_CHECKING:
    from stabilize.queue.messages import Message


class MessageContractError(ValueError):
    """A persisted message does not match its declared field types.

    The queue row was written by another process, and on a rolling deploy by
    another VERSION. Splatting it into a dataclass checks field NAMES and no
    field TYPE, so a wrong value is admitted here and detonates later inside a
    handler -- reaching the DLQ attributed to whatever code touched it rather
    than to the deserialization that accepted it.
    """


_ADAPTERS: dict[type[Message], Any] = {}


def _adapter_for(message_class: type[Message]) -> Any:
    adapter = _ADAPTERS.get(message_class)
    if adapter is None:
        adapter = _ADAPTERS.setdefault(message_class, TypeAdapter(message_class))
    return adapter
