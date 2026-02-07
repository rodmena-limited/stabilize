"""
Event recorder service.

Records events for all state transitions in the workflow system.
Provides context tracking for correlation and causation.
"""

from __future__ import annotations

import logging
import threading
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from stabilize.events.base import (
    EntityType,
    Event,
    EventMetadata,
    EventType,
    create_stage_event,
    create_task_event,
    create_workflow_event,
)
from stabilize.events.bus import get_event_bus

if TYPE_CHECKING:
    from stabilize.events.store.interface import EventStore
    from stabilize.models.stage import StageExecution
    from stabilize.models.status import WorkflowStatus
    from stabilize.models.task import TaskExecution
    from stabilize.models.workflow import Workflow

logger = logging.getLogger(__name__)


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


class EventRecorder:
    """
    Records events for all state transitions.

    Integrates with EventStore for persistence and EventBus for
    pub/sub notifications. All record methods return the created
    event with its assigned sequence number.
    """

    def __init__(
        self,
        event_store: EventStore,
        publish_to_bus: bool = True,
    ) -> None:
        """
        Initialize the event recorder.

        Args:
            event_store: Store for persisting events.
            publish_to_bus: Whether to also publish events to the bus.
        """
        self._event_store = event_store
        self._publish_to_bus = publish_to_bus
        self._lock = threading.Lock()

    def _record(
        self,
        event: Event,
        connection: Any | None = None,
    ) -> Event:
        """
        Record an event to store and optionally publish to bus.

        Args:
            event: Event to record.
            connection: Optional DB connection for transaction.

        Returns:
            Event with sequence assigned.
        """
        recorded = self._event_store.append(event, connection=connection)

        if self._publish_to_bus:
            try:
                get_event_bus().publish(recorded)
            except Exception as e:
                logger.warning("Failed to publish event to bus: %s", e)

        return recorded

    def _record_batch(
        self,
        events: list[Event],
        connection: Any | None = None,
    ) -> list[Event]:
        """Record multiple events atomically."""
        if not events:
            return []

        recorded = self._event_store.append_batch(events, connection=connection)

        if self._publish_to_bus:
            try:
                get_event_bus().publish_batch(recorded)
            except Exception as e:
                logger.warning("Failed to publish events to bus: %s", e)

        return recorded

    # ========== Workflow Events ==========

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

    # ========== Stage Events ==========

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

    # ========== Task Events ==========

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

    # ========== Generic Events ==========

    def record_status_change(
        self,
        entity_type: EntityType,
        entity_id: str,
        workflow_id: str,
        old_status: WorkflowStatus,
        new_status: WorkflowStatus,
        version: int = 1,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record a generic status change event."""
        event = Event(
            event_type=EventType.STATUS_CHANGED,
            entity_type=entity_type,
            entity_id=entity_id,
            workflow_id=workflow_id,
            version=version,
            data={
                "old_status": old_status.name,
                "new_status": new_status.name,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_context_updated(
        self,
        entity_type: EntityType,
        entity_id: str,
        workflow_id: str,
        context: dict[str, Any],
        version: int = 1,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record a context update event."""
        event = Event(
            event_type=EventType.CONTEXT_UPDATED,
            entity_type=entity_type,
            entity_id=entity_id,
            workflow_id=workflow_id,
            version=version,
            data={"context": context},
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_outputs_updated(
        self,
        entity_type: EntityType,
        entity_id: str,
        workflow_id: str,
        outputs: dict[str, Any],
        version: int = 1,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record an outputs update event."""
        event = Event(
            event_type=EventType.OUTPUTS_UPDATED,
            entity_type=entity_type,
            entity_id=entity_id,
            workflow_id=workflow_id,
            version=version,
            data={"outputs": outputs},
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)

    def record_jump_executed(
        self,
        workflow_id: str,
        from_stage_id: str,
        to_stage_id: str,
        jump_type: str,
        version: int = 1,
        connection: Any | None = None,
        source_handler: str | None = None,
    ) -> Event:
        """Record a jump/restart execution event."""
        event = Event(
            event_type=EventType.JUMP_EXECUTED,
            entity_type=EntityType.WORKFLOW,
            entity_id=workflow_id,
            workflow_id=workflow_id,
            version=version,
            data={
                "from_stage_id": from_stage_id,
                "to_stage_id": to_stage_id,
                "jump_type": jump_type,
            },
            metadata=get_event_metadata(source_handler),
        )
        return self._record(event, connection)


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
    global _event_recorder
    with _recorder_lock:
        _event_recorder = EventRecorder(event_store, publish_to_bus=publish_to_bus)
    return _event_recorder


def reset_event_recorder() -> None:
    """Reset the global event recorder (for testing)."""
    global _event_recorder
    with _recorder_lock:
        _event_recorder = None
