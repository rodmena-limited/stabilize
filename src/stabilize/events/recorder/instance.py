"""
Global event recorder instance management.

Provides get_event_recorder(), configure_event_recorder(), and
reset_event_recorder() for managing the singleton EventRecorder.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.events.recorder.composed import EventRecorder
    from stabilize.events.store.interface import EventStore

# Global event recorder instance
_event_recorder: EventRecorder | None = None
_recorder_lock = threading.Lock()


def get_event_recorder() -> EventRecorder | None:
    """Get the global event recorder instance, if configured."""
    return _event_recorder


def configure_event_recorder(
    event_store: EventStore,
    publish_to_bus: bool = True,
) -> EventRecorder:
    """
    Configure the global event recorder.

    Args:
        event_store: Store for persisting events.
        publish_to_bus: Whether to publish events to the bus.

    Returns:
        The configured event recorder.
    """
    from stabilize.events.recorder.composed import EventRecorder

    global _event_recorder
    with _recorder_lock:
        _event_recorder = EventRecorder(event_store, publish_to_bus=publish_to_bus)
    return _event_recorder


def reset_event_recorder() -> None:
    """Reset the global event recorder (for testing)."""
    global _event_recorder
    with _recorder_lock:
        _event_recorder = None
