"""
Event recorder service.

Records events for all state transitions in the workflow system.
Provides context tracking for correlation and causation.

This package re-exports everything that was previously importable from
the stabilize.events.recorder module, preserving backwards compatibility.
"""

from stabilize.events.recorder.composed import EventRecorder
from stabilize.events.recorder.context import get_event_metadata, set_event_context
from stabilize.events.recorder.instance import (
    configure_event_recorder,
    get_event_recorder,
    reset_event_recorder,
)

__all__ = [
    "EventRecorder",
    "configure_event_recorder",
    "get_event_metadata",
    "get_event_recorder",
    "reset_event_recorder",
    "set_event_context",
]
