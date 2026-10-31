from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.models.status import WorkflowStatus
    from stabilize.models.workflow import Workflow


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

    def record_status_transition(
        self,
        entity: str,
        entity_id: str,
        workflow_id: str,
        old_status: WorkflowStatus,
        new_status: WorkflowStatus,
        version: int,
    ) -> None:
        """Record status.changed inside the open store transaction, or inside the next one this thread opens."""
        recorder = self.event_recorder
        if recorder is None or not workflow_id or old_status == new_status:
            return
        from stabilize.events.base import EntityType

        recorder.record_status_change(
            EntityType(entity),
            entity_id,
            workflow_id,
            old_status,
            new_status,
            version=version,
            source_handler=type(self).__name__,
            held=True,
        )

    def record_workflow_resumed(self, workflow: Workflow, source_handler: str) -> None:
        recorder = self.event_recorder
        if recorder is not None:
            recorder.record_workflow_resumed(workflow, source_handler=source_handler)
