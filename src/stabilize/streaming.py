"""
Streaming surface for live workflow progress (agentic ergonomics).

LangGraph exposes ``stream``/``astream`` to feed live values, state updates,
and LLM tokens to a caller. Stabilize already has the plumbing — an
event-sourced log plus a thread-safe :class:`~stabilize.events.bus.EventBus`
— so this module only adds the missing consumer surface:

- :func:`emit_progress` lets a task push a custom progress event (a step
  narration, an LLM token chunk, a percentage) from inside ``execute``.
- :class:`WorkflowStream` lets a caller consume a workflow's events, either
  live (subscribe to the bus) or by replaying the durable log, or both
  (replay history, then follow live) — all additive and opt-in.

Nothing here changes existing behavior: workflows that never call
``emit_progress`` and callers that never open a ``WorkflowStream`` are
unaffected.
"""

from __future__ import annotations

import queue as _queue
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from stabilize.events.base import EntityType, Event, EventType
from stabilize.events.bus import SubscriptionMode, get_event_bus

if TYPE_CHECKING:
    from stabilize.events.recorder import EventRecorder
    from stabilize.events.store.interface import EventStore
    from stabilize.models.stage import StageExecution


@dataclass
class StreamItem:
    """One item yielded by a :class:`WorkflowStream`."""

    event_type: str
    entity_type: str
    entity_id: str
    workflow_id: str
    data: dict[str, Any]
    sequence: int
    source: str  # "replay" or "live"

    @classmethod
    def from_event(cls, event: Event, source: str) -> StreamItem:
        return cls(
            event_type=event.event_type.value if event.event_type else "",
            entity_type=event.entity_type.value if event.entity_type else "",
            entity_id=event.entity_id,
            workflow_id=event.workflow_id,
            data=dict(event.data),
            sequence=event.sequence,
            source=source,
        )


def emit_progress(
    stage: StageExecution,
    message: str = "",
    *,
    recorder: EventRecorder | None = None,
    **data: Any,
) -> None:
    """Emit a custom progress event for a running stage.

    Publishes a CUSTOM event carrying ``message`` and any extra keyword data
    to the event bus (and, if a recorder is given, the durable log). Callers
    watching a :class:`WorkflowStream` see it live. Safe to call from a task's
    ``execute`` — if event sourcing is not configured, this is a cheap no-op
    against the always-available global bus.

    Args:
        stage: The stage emitting progress (must have an execution).
        message: Human-readable progress line.
        recorder: Optional recorder to also persist the event durably.
        **data: Extra structured fields (e.g. token="...", percent=42).
    """
    workflow_id = stage.execution.id if stage.execution else ""
    payload: dict[str, Any] = {"message": message, "stage_ref_id": stage.ref_id}
    payload.update(data)
    event = Event(
        event_type=EventType.CUSTOM,
        entity_type=EntityType.STAGE,
        entity_id=stage.id,
        workflow_id=workflow_id,
        data=payload,
    )
    if recorder is not None:
        # Recorder handles bus publication (deferred inside a txn scope).
        recorder._record(event)
    else:
        get_event_bus().publish(event)


class WorkflowStream:
    """Consume a workflow's events live and/or from the durable log.

    Example (live follow until the workflow ends)::

        stream = WorkflowStream(execution_id, event_store=store)
        for item in stream.follow():
            print(item.event_type, item.data.get("message", ""))

    Example (callback form, non-blocking)::

        stream = WorkflowStream(execution_id)
        stream.on_event(lambda item: print(item))
        ...  # run the workflow
        stream.close()
    """

    _TERMINAL_EVENTS = {
        EventType.WORKFLOW_COMPLETED.value,
        EventType.WORKFLOW_FAILED.value,
        EventType.WORKFLOW_CANCELED.value,
    }

    def __init__(
        self,
        execution_id: str,
        event_store: EventStore | None = None,
        subscription_id: str | None = None,
    ) -> None:
        """Args:
        execution_id: Workflow to stream.
        event_store: Optional durable store for replaying history. Required
            for :meth:`replay`; live streaming works without it.
        subscription_id: Bus subscription id (defaults to a per-instance id).
        """
        self.execution_id = execution_id
        self._event_store = event_store
        self._sub_id = subscription_id or f"stream-{id(self)}-{execution_id}"
        self._subscribed = False

    # ---- History ----

    def replay(self) -> Iterator[StreamItem]:
        """Yield all durably-recorded events for this workflow, in order."""
        if self._event_store is None:
            raise ValueError("replay() requires an event_store")
        for event in self._event_store.get_events_for_workflow(self.execution_id):
            yield StreamItem.from_event(event, source="replay")

    # ---- Live ----

    def on_event(self, callback: Any, *, mode: SubscriptionMode = SubscriptionMode.SYNC) -> None:
        """Register a callback invoked with each live :class:`StreamItem`.

        Non-blocking. Call :meth:`close` to unsubscribe.
        """
        bus = get_event_bus()

        def _handler(event: Event) -> None:
            if event.workflow_id == self.execution_id:
                callback(StreamItem.from_event(event, source="live"))

        bus.subscribe(self._sub_id, _handler, workflow_filter=self.execution_id, mode=mode)
        self._subscribed = True

    def follow(self, include_history: bool = False, timeout: float | None = None) -> Iterator[StreamItem]:
        """Yield events for this workflow as they occur, until it ends.

        Args:
            include_history: First replay the durable log (requires an
                event_store), then continue live. Events already replayed are
                de-duplicated against the live feed by sequence.
            timeout: Stop after this many seconds of no new events (None waits
                indefinitely, but always stops on a workflow-terminal event).
        """
        live_q: _queue.Queue[Event] = _queue.Queue()
        bus = get_event_bus()

        def _handler(event: Event) -> None:
            if event.workflow_id == self.execution_id:
                live_q.put(event)

        bus.subscribe(self._sub_id, _handler, workflow_filter=self.execution_id, mode=SubscriptionMode.SYNC)
        self._subscribed = True

        seen_sequences: set[int] = set()
        try:
            if include_history and self._event_store is not None:
                for event in self._event_store.get_events_for_workflow(self.execution_id):
                    if event.sequence:
                        seen_sequences.add(event.sequence)
                    item = StreamItem.from_event(event, source="replay")
                    yield item
                    if item.event_type in self._TERMINAL_EVENTS:
                        return

            while True:
                try:
                    event = live_q.get(timeout=timeout) if timeout else live_q.get()
                except _queue.Empty:
                    return
                if event.sequence and event.sequence in seen_sequences:
                    continue
                item = StreamItem.from_event(event, source="live")
                yield item
                if item.event_type in self._TERMINAL_EVENTS:
                    return
        finally:
            self.close()

    def close(self) -> None:
        """Unsubscribe from the live event bus."""
        if self._subscribed:
            try:
                get_event_bus().unsubscribe(self._sub_id)
            except Exception:
                pass
            self._subscribed = False

    def __enter__(self) -> WorkflowStream:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
