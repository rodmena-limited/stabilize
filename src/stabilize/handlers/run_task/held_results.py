"""Results of tasks that completed but could not be saved, kept for the redelivery."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.models.task import TaskExecution
    from stabilize.tasks.result import TaskResult

_held: dict[str, tuple[int | None, TaskResult]] = {}
_lock = threading.Lock()


def hold(task_model: TaskExecution, result: TaskResult) -> None:
    """Keep result for the next delivery of this execution of the task."""
    with _lock:
        _held[task_model.id] = (task_model.start_time, result)


def take(task_model: TaskExecution) -> TaskResult | None:
    """Return and forget the held result, only if it belongs to this execution."""
    with _lock:
        held = _held.pop(task_model.id, None)
    if held is None or held[0] != task_model.start_time:
        return None
    return held[1]


def clear() -> None:
    with _lock:
        _held.clear()


_UNAVAILABLE_TYPE_NAMES = frozenset(
    {"OperationalError", "InterfaceError", "PoolTimeout", "PoolClosed", "ConcurrencyError", "TooManyConnections"}
)


def is_store_unavailable(error: BaseException) -> bool:
    """True when a failed save may succeed on retry: the store, not the data, was the problem."""
    from stabilize.errors import is_transient

    if isinstance(error, (ConnectionError, TimeoutError)):
        return True
    if isinstance(error, Exception) and is_transient(error):
        return True
    return any(cls.__name__ in _UNAVAILABLE_TYPE_NAMES for cls in type(error).__mro__)
