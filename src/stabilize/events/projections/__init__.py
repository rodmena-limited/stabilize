"""Event projections for building read models.

Projections apply events to build specialized views of the data
that are optimized for specific query patterns.
"""

from __future__ import annotations

from stabilize.events.projections.base import Projection
from stabilize.events.projections.metrics import StageMetrics, StageMetricsProjection
from stabilize.events.projections.timeline import (
    TimelineEntry,
    WorkflowTimeline,
    WorkflowTimelineProjection,
)

__all__ = [
    "Projection",
    "StageMetrics",
    "StageMetricsProjection",
    "TimelineEntry",
    "WorkflowTimeline",
    "WorkflowTimelineProjection",
]
