"""
Task event recording mixin.

Provides record_task_* methods for the EventRecorder.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.events.base import EventType, create_task_event
from stabilize.events.recorder.context import get_event_metadata

if TYPE_CHECKING:
    from stabilize.events.base import Event
    from stabilize.models.task import TaskExecution


class TaskEventsMixin:
    """Mixin providing task event recording methods."""

    def record_task_started(
        self,
        task: TaskExecution,
        workflow_id: str,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record task started event."""
        event = create_task_event(
            event_type=EventType.TASK_STARTED,
            task_id=task.id,
            workflow_id=workflow_id,
            version=task.version,
            data={
                "name": task.name,
                "implementing_class": task.implementing_class,
                "start_time": task.start_time,
                "stage_id": task.stage.id if task.stage else None,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_task_completed(
        self,
        task: TaskExecution,
        workflow_id: str,
        outputs: dict[str, Any] | None = None,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record task completion event."""
        event = create_task_event(
            event_type=EventType.TASK_COMPLETED,
            task_id=task.id,
            workflow_id=workflow_id,
            version=task.version,
            data={
                "name": task.name,
                "status": task.status.name,
                "end_time": task.end_time,
                "duration_ms": (task.end_time - task.start_time if task.start_time and task.end_time else None),
                "outputs": outputs or {},
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_task_failed(
        self,
        task: TaskExecution,
        workflow_id: str,
        error: str,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record task failure event."""
        event = create_task_event(
            event_type=EventType.TASK_FAILED,
            task_id=task.id,
            workflow_id=workflow_id,
            version=task.version,
            data={
                "name": task.name,
                "status": task.status.name,
                "error": error,
                "end_time": task.end_time,
                "exception_details": task.task_exception_details or None,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_task_retried(
        self,
        task: TaskExecution,
        workflow_id: str,
        attempt: int,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record task retry event."""
        event = create_task_event(
            event_type=EventType.TASK_RETRIED,
            task_id=task.id,
            workflow_id=workflow_id,
            version=task.version,
            data={
                "name": task.name,
                "attempt": attempt,
                "last_error": task.exception_details.exception if task.exception_details else None,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)
