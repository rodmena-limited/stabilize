"""
Global event recorder instance management.

Provides get_event_recorder(), configure_event_recorder(), and
reset_event_recorder() for managing the singleton EventRecorder.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.events.recorder.composed import EventRecorder
    from stabilize.events.store.interface import EventStore
    from stabilize.models.workflow import PausedDetails

# Global event recorder instance
_event_recorder: EventRecorder | None = None
logger = logging.getLogger(__name__)

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


def recorder_for(tenant: str) -> EventRecorder | None:
    """The global recorder when it records the given tenant's store, else None."""
    recorder = get_event_recorder()
    if recorder is None:
        return None
    store_tenant = getattr(getattr(recorder, "_event_store", None), "tenant", "") or ""
    if store_tenant != (tenant or ""):
        logger.debug("No recorder for tenant %r: the configured recorder records tenant %r", tenant, store_tenant)
        return None
    return recorder


def record_workflow_paused_for(tenant: str, workflow_id: str, paused: PausedDetails) -> None:
    recorder = recorder_for(tenant)
    if recorder is not None:
        recorder.record_workflow_paused(
            workflow_id=workflow_id,
            paused_by=paused.paused_by,
            pause_time=paused.pause_time,
            source_handler="WorkflowStore.pause",
        )


def record_workflow_resumed_for(
    tenant: str, workflow_id: str, paused: PausedDetails | None, source_handler: str
) -> None:
    recorder = recorder_for(tenant)
    if recorder is not None:
        recorder.record_workflow_resumed(
            workflow_id=workflow_id,
            resume_time=paused.resume_time if paused else None,
            paused_ms=paused.paused_ms if paused else None,
            source_handler=source_handler,
        )
