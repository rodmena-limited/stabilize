"""
Stage event recording mixin.

Provides record_stage_* methods for the EventRecorder.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.events.base import EventType, create_stage_event
from stabilize.events.recorder.context import get_event_metadata

if TYPE_CHECKING:
    from stabilize.events.base import Event
    from stabilize.models.stage import StageExecution


class StageEventsMixin:
    """Mixin providing stage event recording methods."""

    if TYPE_CHECKING:

        def _record(self, event: Event, connection: Any | None = None) -> Event: ...

    def record_stage_started(
        self,
        stage: StageExecution,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record stage started event."""
        event = create_stage_event(
            event_type=EventType.STAGE_STARTED,
            stage_id=stage.id,
            workflow_id=stage.execution.id if stage.execution else "",
            version=stage.version,
            data={
                "ref_id": stage.ref_id,
                "type": stage.type,
                "name": stage.name,
                "start_time": stage.start_time,
                "is_synthetic": stage.is_synthetic(),
                "parent_stage_id": stage.parent_stage_id,
                "task_count": len(stage.tasks),
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_stage_completed(
        self,
        stage: StageExecution,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record stage completion event."""
        event = create_stage_event(
            event_type=EventType.STAGE_COMPLETED,
            stage_id=stage.id,
            workflow_id=stage.execution.id if stage.execution else "",
            version=stage.version,
            data={
                "ref_id": stage.ref_id,
                "type": stage.type,
                "name": stage.name,
                "status": stage.status.name,
                "end_time": stage.end_time,
                "duration_ms": (stage.end_time - stage.start_time if stage.start_time and stage.end_time else None),
                "outputs": stage.outputs,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_stage_failed(
        self,
        stage: StageExecution,
        error: str,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record stage failure event."""
        event = create_stage_event(
            event_type=EventType.STAGE_FAILED,
            stage_id=stage.id,
            workflow_id=stage.execution.id if stage.execution else "",
            version=stage.version,
            data={
                "ref_id": stage.ref_id,
                "type": stage.type,
                "name": stage.name,
                "status": stage.status.name,
                "error": error,
                "end_time": stage.end_time,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_stage_skipped(
        self,
        stage: StageExecution,
        reason: str,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record stage skipped event."""
        event = create_stage_event(
            event_type=EventType.STAGE_SKIPPED,
            stage_id=stage.id,
            workflow_id=stage.execution.id if stage.execution else "",
            version=stage.version,
            data={
                "ref_id": stage.ref_id,
                "type": stage.type,
                "name": stage.name,
                "reason": reason,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_stage_canceled(
        self,
        stage: StageExecution,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record stage canceled event."""
        event = create_stage_event(
            event_type=EventType.STAGE_CANCELED,
            stage_id=stage.id,
            workflow_id=stage.execution.id if stage.execution else "",
            version=stage.version,
            data={
                "ref_id": stage.ref_id,
                "type": stage.type,
                "name": stage.name,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)
