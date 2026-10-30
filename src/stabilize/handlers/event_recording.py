from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder


class EventRecordingMixin:
    _event_recorder: EventRecorder | None

    # ========== Event Recording ==========

    @property
    def event_recorder(self) -> EventRecorder | None:
        """Get the event recorder, preferring instance over global."""
        if self._event_recorder is not None:
            return self._event_recorder
        # Fall back to global recorder if available
        from stabilize.events.recorder import get_event_recorder

        return get_event_recorder()

    def set_event_context(
        self,
        workflow_id: str,
        *,
        actor: str | None = None,
        causation_id: str | None = None,
    ) -> None:
        """Set event context for correlation tracking.

        Call this at the start of handler processing to establish
        correlation context for any events recorded during handling.

        Args:
            workflow_id: The workflow ID for correlation.
            actor: Who triggered this action, where a real identity is known.
                Defaults to "system".
            causation_id: The id of the event that caused this action.
        """
        from stabilize.events.recorder import set_event_context

        set_event_context(
            correlation_id=workflow_id,
            causation_id=causation_id,
            actor=actor or "system",
        )
