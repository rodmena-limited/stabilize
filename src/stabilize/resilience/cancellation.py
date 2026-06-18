"""Cooperative cancellation for long-running tasks.

Python cannot forcibly kill a running thread, so thread-mode timeouts and
stage cancellations cannot interrupt a task that is already mid-execution. This
module gives well-behaved tasks a way to *cooperatively* notice cancellation and
stop early.

A task checks cancellation via the no-argument helpers, which read a context
variable the engine binds for the duration of the task's execution::

    from stabilize.resilience.cancellation import is_cancellation_requested

    class LongTask(Task):
        def execute(self, stage):
            for item in items:
                if is_cancellation_requested():
                    return TaskResult.terminal("Canceled")
                process(item)
            return TaskResult.success()

This is fully opt-in: tasks that never check cancellation behave exactly as
before. For *hard* interruption guarantees (terminating a runaway task that does
not cooperate), run tasks in process-isolation mode
(``STABILIZE_ISOLATION_MODE=process``), which can terminate the worker process.
"""

from __future__ import annotations

import threading
from contextvars import ContextVar, Token


class TaskCancelledError(Exception):
    """Raised by ``raise_if_cancellation_requested`` when cancellation is set.

    Treated as a terminal (non-transient) error by the task runner, so a
    cancelled task is not retried.
    """


class CancellationToken:
    """A thread-safe one-shot cancellation flag."""

    __slots__ = ("_event",)

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        """Signal cancellation. Idempotent."""
        self._event.set()

    @property
    def is_cancelled(self) -> bool:
        """True once cancel() has been called."""
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        """Raise TaskCancelledError if cancellation has been signaled."""
        if self._event.is_set():
            raise TaskCancelledError("Task execution was cancelled")


# --- Registry: maps task_id -> token so the engine can signal a running task ---
_registry: dict[str, CancellationToken] = {}
_registry_lock = threading.Lock()


def register_token(task_id: str) -> CancellationToken:
    """Create and register a fresh cancellation token for a task."""
    token = CancellationToken()
    with _registry_lock:
        _registry[task_id] = token
    return token


def get_token(task_id: str) -> CancellationToken | None:
    """Return the registered token for a task, if any."""
    with _registry_lock:
        return _registry.get(task_id)


def cancel_task(task_id: str) -> bool:
    """Signal cancellation for a running task. Returns True if a token existed."""
    with _registry_lock:
        token = _registry.get(task_id)
    if token is not None:
        token.cancel()
        return True
    return False


def unregister_token(task_id: str) -> None:
    """Remove a task's token from the registry (no error if absent)."""
    with _registry_lock:
        _registry.pop(task_id, None)


def reset_cancellation_state() -> None:
    """Clear the token registry. Primarily for test isolation."""
    with _registry_lock:
        _registry.clear()


# --- Per-execution current token, bound inside the task's worker thread ---
_current_token: ContextVar[CancellationToken | None] = ContextVar("stabilize_current_token", default=None)


def bind_current_token(token: CancellationToken | None) -> Token[CancellationToken | None]:
    """Bind the current-thread cancellation token; returns a reset handle."""
    return _current_token.set(token)


def reset_current_token(reset_handle: Token[CancellationToken | None]) -> None:
    """Restore the previous current-token binding."""
    _current_token.reset(reset_handle)


def current_cancellation_token() -> CancellationToken | None:
    """Return the cancellation token bound for the current task, if any."""
    return _current_token.get()


def is_cancellation_requested() -> bool:
    """True if the current task's cancellation has been requested.

    Safe to call from anywhere — returns False when no token is bound.
    """
    token = _current_token.get()
    return token is not None and token.is_cancelled


def raise_if_cancellation_requested() -> None:
    """Raise TaskCancelledError if the current task has been cancelled."""
    token = _current_token.get()
    if token is not None:
        token.raise_if_cancelled()
