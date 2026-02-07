"""
Workflow event recording mixin.

Provides record_workflow_* methods for the EventRecorder.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.events.base import EventType, create_workflow_event
from stabilize.events.recorder.context import get_event_metadata

if TYPE_CHECKING:
    from stabilize.events.base import Event
    from stabilize.models.workflow import Workflow


class WorkflowEventsMixin:
    """Mixin providing workflow event recording methods."""

    def record_workflow_created(
        self,
        workflow: Workflow,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record workflow creation event."""
        event = create_workflow_event(
            event_type=EventType.WORKFLOW_CREATED,
            workflow_id=workflow.id,
            version=1,
            data={
                "application": workflow.application,
                "name": workflow.name,
                "type": workflow.type.value,
                "trigger_type": workflow.trigger.type if workflow.trigger else "manual",
                "stage_count": len(workflow.stages),
                "initial_stage_count": len(workflow.initial_stages()),
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_workflow_started(
        self,
        workflow: Workflow,
        initial_stage_ids: list[str] | None = None,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record workflow started event."""
        event = create_workflow_event(
            event_type=EventType.WORKFLOW_STARTED,
            workflow_id=workflow.id,
            version=2,
            data={
                "start_time": workflow.start_time,
                "initial_stage_ids": initial_stage_ids or [],
                "context": workflow.context,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_workflow_completed(
        self,
        workflow: Workflow,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record workflow completion event."""
        event = create_workflow_event(
            event_type=EventType.WORKFLOW_COMPLETED,
            workflow_id=workflow.id,
            version=3,
            data={
                "status": workflow.status.name,
                "end_time": workflow.end_time,
                "duration_ms": (
                    workflow.end_time - workflow.start_time if workflow.start_time and workflow.end_time else None
                ),
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_workflow_failed(
        self,
        workflow: Workflow,
        error: str,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record workflow failure event."""
        event = create_workflow_event(
            event_type=EventType.WORKFLOW_FAILED,
            workflow_id=workflow.id,
            version=3,
            data={
                "status": workflow.status.name,
                "error": error,
                "end_time": workflow.end_time,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_workflow_canceled(
        self,
        workflow: Workflow,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record workflow cancellation event."""
        event = create_workflow_event(
            event_type=EventType.WORKFLOW_CANCELED,
            workflow_id=workflow.id,
            version=3,
            data={
                "canceled_by": workflow.canceled_by,
                "reason": workflow.cancellation_reason,
                "end_time": workflow.end_time,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_workflow_paused(
        self,
        workflow: Workflow,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record workflow paused event."""
        event = create_workflow_event(
            event_type=EventType.WORKFLOW_PAUSED,
            workflow_id=workflow.id,
            version=workflow.stages[0].version if workflow.stages else 1,
            data={
                "paused_by": workflow.paused.paused_by if workflow.paused else None,
                "pause_time": workflow.paused.pause_time if workflow.paused else None,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_workflow_resumed(
        self,
        workflow: Workflow,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record workflow resumed event."""
        event = create_workflow_event(
            event_type=EventType.WORKFLOW_RESUMED,
            workflow_id=workflow.id,
            version=workflow.stages[0].version if workflow.stages else 1,
            data={
                "resume_time": workflow.paused.resume_time if workflow.paused else None,
                "paused_ms": workflow.paused.paused_ms if workflow.paused else None,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)
