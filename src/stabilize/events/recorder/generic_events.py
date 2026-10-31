"""
Generic event recording mixin.

Provides record_status_change, record_context_updated,
record_outputs_updated, and record_jump_executed methods.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.events.base import EntityType, Event, EventType
from stabilize.events.recorder.context import get_event_metadata
from stabilize.events.recorder.digests import change_payload

if TYPE_CHECKING:
    from stabilize.models.status import WorkflowStatus


class GenericEventsMixin:
    """Mixin providing generic event recording methods."""

    if TYPE_CHECKING:
        _audit_key: bytes | None

        def _record(self, event: Event, connection: Any | None = None) -> Event: ...

        def _record_held(self, event: Event) -> None: ...

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
        held: bool = False,
    ) -> Event | None:
        """Record a status transition; with *held*, inside the store transaction that next opens on this thread."""
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
        if held:
            self._record_held(event)
            return None
        return self._record(event, connection)

    def record_context_updated(
        self,
        entity_type: EntityType,
        entity_id: str,
        workflow_id: str,
        changed: dict[str, Any],
        version: int = 1,
        connection: Any | None = None,
        source_handler: str | None = None,
        held: bool = False,
    ) -> Event | None:
        """Record which context keys changed; values are never stored, only their digests under an audit key."""
        event = Event(
            event_type=EventType.CONTEXT_UPDATED,
            entity_type=entity_type,
            entity_id=entity_id,
            workflow_id=workflow_id,
            version=version,
            data=change_payload(changed, self._audit_key),
            metadata=get_event_metadata(source_handler),
        )
        if held:
            self._record_held(event)
            return None
        return self._record(event, connection)

    def record_outputs_updated(
        self,
        entity_type: EntityType,
        entity_id: str,
        workflow_id: str,
        changed: dict[str, Any],
        version: int = 1,
        connection: Any | None = None,
        source_handler: str | None = None,
        held: bool = False,
    ) -> Event | None:
        """Record which output keys changed; values are never stored, only their digests under an audit key."""
        event = Event(
            event_type=EventType.OUTPUTS_UPDATED,
            entity_type=entity_type,
            entity_id=entity_id,
            workflow_id=workflow_id,
            version=version,
            data=change_payload(changed, self._audit_key),
            metadata=get_event_metadata(source_handler),
        )
        if held:
            self._record_held(event)
            return None
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
