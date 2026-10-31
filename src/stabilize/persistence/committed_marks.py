"""What the current delivery's transactions committed, per thread: marks, pushes, and pushes outside any transaction."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

_local = threading.local()

DEFECT_LIMIT = 200


@dataclass
class DeliveryDefect:
    message_type: str
    message_id: str | None
    reasons: tuple[str, ...]


@dataclass
class _Delivery:
    marked: set[str] = field(default_factory=set)
    pushes: list[tuple[str, ...]] = field(default_factory=list)
    bare_pushes: list[str] = field(default_factory=list)
    exempt: int = 0


def _state() -> _Delivery:
    state: _Delivery | None = getattr(_local, "delivery", None)
    if state is None:
        state = _Delivery()
        _local.delivery = state
    return state


def record_committed(message_ids: Iterable[str], pushed: bool = False) -> None:
    """Record a committed transaction: the ids it marked processed, and whether it pushed."""
    state = _state()
    marked = tuple(message_ids)
    state.marked.update(marked)
    if pushed:
        state.pushes.append(marked)


def record_bare_push(message_type: str) -> None:
    """Record a push made on the queue's own connection, outside any store transaction."""
    state = _state()
    if state.exempt == 0:
        state.bare_pushes.append(message_type)


@contextmanager
def delivery_exempt() -> Iterator[None]:
    """Pushes in this block belong to no handler: the orchestrator, recovery, signalling, NoOpTransaction."""
    state = _state()
    state.exempt += 1
    try:
        yield
    finally:
        state.exempt -= 1


def consume_committed(message_id: str) -> bool:
    """Return True, and forget it, when *message_id* was marked by a committed transaction on this thread."""
    state = _state()
    if message_id in state.marked:
        state.marked.discard(message_id)
        return True
    return False


def delivery_defects(message_id: str | None) -> list[str]:
    """How this delivery broke the contract; empty when every committed push sat beside its mark."""
    state = _state()
    reasons = [f"{kind} was pushed outside any store transaction" for kind in state.bare_pushes]
    if message_id is not None:
        for marked in state.pushes:
            if message_id not in marked:
                marks = f"; it marked {', '.join(marked)}" if marked else ""
                reasons.append(f"a transaction pushed without marking {message_id}{marks}")
    return reasons


def strict_delivery() -> bool:
    return os.environ.get("STABILIZE_STRICT_DELIVERY", "").strip().lower() not in ("", "0", "false", "no")


def clear_committed() -> None:
    """Forget what the previous delivery committed on this thread."""
    state = _state()
    state.marked.clear()
    state.pushes.clear()
    state.bare_pushes.clear()
