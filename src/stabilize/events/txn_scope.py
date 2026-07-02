"""
Thread-local store-transaction scope for event recording.

The workflow stores bind a scope for the duration of a ``store.transaction``
block. While a scope is active on the current thread:

- Event appends targeting the SAME database join the transaction's
  connection, so events commit or roll back together with the workflow
  state they describe (no phantom events).
- Bus publication is deferred until after the transaction commits, so
  subscribers never observe state that was rolled back. On rollback the
  pending publications are dropped along with the appends.

Appends targeting a DIFFERENT database cannot join the transaction; they are
appended standalone (documented eventual-consistency semantics) but their
bus publication is still deferred to commit.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.events.base import Event

logger = logging.getLogger(__name__)


class TxnScope:
    """State of the store transaction active on the current thread."""

    __slots__ = ("connection", "url", "pending", "depth")

    def __init__(self, connection: Any, url: str | None) -> None:
        self.connection = connection
        self.url = url
        self.pending: list[Event] = []
        self.depth = 1


_local = threading.local()


def begin_store_transaction(connection: Any, url: str | None) -> None:
    """Bind a transaction scope to the current thread (re-entrant)."""
    scope: TxnScope | None = getattr(_local, "scope", None)
    if scope is not None:
        scope.depth += 1
        return
    _local.scope = TxnScope(connection, url)


def current_scope() -> TxnScope | None:
    """Return the scope bound to the current thread, if any."""
    return getattr(_local, "scope", None)


def commit_store_transaction() -> None:
    """Unbind the scope after COMMIT and publish deferred events."""
    scope: TxnScope | None = getattr(_local, "scope", None)
    if scope is None:
        return
    scope.depth -= 1
    if scope.depth > 0:
        return
    _local.scope = None
    if scope.pending:
        from stabilize.events.bus import get_event_bus

        bus = get_event_bus()
        for event in scope.pending:
            try:
                bus.publish(event)
            except Exception as e:
                logger.warning("Failed to publish deferred event to bus: %s", e)


def abort_store_transaction() -> None:
    """Unbind the scope after ROLLBACK, dropping deferred publications."""
    scope: TxnScope | None = getattr(_local, "scope", None)
    if scope is None:
        return
    scope.depth -= 1
    if scope.depth > 0:
        return
    _local.scope = None
    if scope.pending:
        logger.debug(
            "Dropped %d deferred event publication(s) after transaction rollback",
            len(scope.pending),
        )
