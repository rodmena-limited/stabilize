"""
Composed EventRecorder class.

Combines all mixin classes into the final EventRecorder that provides
the full API for recording workflow, stage, task, and generic events.
"""

from __future__ import annotations

from stabilize.events.recorder.base import EventRecorderBase
from stabilize.events.recorder.generic_events import GenericEventsMixin
from stabilize.events.recorder.stage_events import StageEventsMixin
from stabilize.events.recorder.task_events import TaskEventsMixin
from stabilize.events.recorder.workflow_events import WorkflowEventsMixin


class EventRecorder(
    WorkflowEventsMixin,
    StageEventsMixin,
    TaskEventsMixin,
    GenericEventsMixin,
    EventRecorderBase,
):
    """
    Records events for all state transitions.

    Integrates with EventStore for persistence and EventBus for
    pub/sub notifications. All record methods return the created
    event with its assigned sequence number.
    """
