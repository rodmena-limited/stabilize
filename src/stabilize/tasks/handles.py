"""The store and queue a task runs under, bound by the RunTask handler for the task's execution and hooks."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from stabilize.persistence.store import WorkflowStore
    from stabilize.queue import Queue


class EngineHandles(NamedTuple):
    store: WorkflowStore
    queue: Queue


_engine: ContextVar[EngineHandles | None] = ContextVar("stabilize_engine_handles", default=None)


def current_engine() -> EngineHandles | None:
    """The processor's store and queue while a task executes or a hook runs; None outside a handler."""
    return _engine.get()


@contextmanager
def bind_engine(store: WorkflowStore, queue: Queue) -> Iterator[None]:
    token = _engine.set(EngineHandles(store, queue))
    try:
        yield
    finally:
        _engine.reset(token)
