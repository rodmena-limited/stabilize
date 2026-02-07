"""
Context variable setup for event causality tracking.

Provides ContextVar-based tracking of correlation IDs, causation IDs,
and actors across async boundaries.
"""

from __future__ import annotations

from contextvars import ContextVar

from stabilize.events.base import EventMetadata

# Context variables for causality tracking
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")
_causation_id: ContextVar[str | None] = ContextVar("causation_id", default=None)
_actor: ContextVar[str] = ContextVar("actor", default="system")


def set_event_context(
    correlation_id: str,
    causation_id: str | None = None,
    actor: str = "system",
) -> None:
    """
    Set the current event context for correlation tracking.

    Args:
        correlation_id: Links related events (typically workflow ID).
        causation_id: ID of the event that caused this action.
        actor: Who triggered this action.
    """
    _correlation_id.set(correlation_id)
    _causation_id.set(causation_id)
    _actor.set(actor)


def get_event_metadata(source_handler: str | None = None) -> EventMetadata:
    """
    Get metadata for a new event from current context.

    Args:
        source_handler: The handler creating this event.

    Returns:
        EventMetadata with current context values.
    """
    return EventMetadata(
        correlation_id=_correlation_id.get(),
        causation_id=_causation_id.get(),
        actor=_actor.get(),
        source_handler=source_handler,
    )
