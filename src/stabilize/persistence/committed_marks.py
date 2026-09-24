"""Message ids whose processed-mark committed inside a handler's own transaction, per thread."""

from __future__ import annotations

import threading
from collections.abc import Iterable

_local = threading.local()


def _committed() -> set[str]:
    ids: set[str] | None = getattr(_local, "ids", None)
    if ids is None:
        ids = set()
        _local.ids = ids
    return ids


def record_committed(message_ids: Iterable[str]) -> None:
    """Record ids marked processed by a transaction that has committed."""
    _committed().update(message_ids)


def consume_committed(message_id: str) -> bool:
    """Return True, and forget it, when *message_id* was recorded on this thread."""
    ids = _committed()
    if message_id in ids:
        ids.discard(message_id)
        return True
    return False


def clear_committed() -> None:
    """Forget every id recorded on this thread."""
    _committed().clear()
